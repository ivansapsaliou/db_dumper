"""
StreamingDumper — memory-efficient parallel table dumping.

Features:
  - Stream dump output without loading everything into memory
  - Parallel table dumping via a thread pool
  - Connection pooling for SSH sessions
  - Incremental compression while dumping (pipe directly to compressor)
  - Configurable thread pool size (default 4 threads)
"""

import io
import os
import gzip
import bz2
import time
import queue
import logging
import threading
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Optional

logger = logging.getLogger(__name__)

CHUNK_SIZE = 65536   # 64 KB


# ── SSH connection pool ───────────────────────────────────────────────────────

class SSHConnectionPool:
    """
    Thread-safe pool of Paramiko SSH clients.
    Connections are created lazily and reused across threads.
    """

    def __init__(self, cfg: dict, max_size: int = 8):
        self._cfg      = cfg
        self._max_size = max_size
        self._pool: queue.Queue = queue.Queue(maxsize=max_size)
        self._lock     = threading.Lock()
        self._count    = 0

    def acquire(self):
        """Get a connection from the pool (creates one if needed)."""
        try:
            return self._pool.get_nowait()
        except queue.Empty:
            with self._lock:
                if self._count < self._max_size:
                    conn = self._make_connection()
                    self._count += 1
                    return conn
            # Pool exhausted — wait for one to be released
            return self._pool.get(timeout=30)

    def release(self, conn):
        """Return a connection to the pool."""
        try:
            self._pool.put_nowait(conn)
        except queue.Full:
            try:
                conn.close()
            except Exception:
                pass

    def close_all(self):
        while True:
            try:
                conn = self._pool.get_nowait()
                try:
                    conn.close()
                except Exception:
                    pass
            except queue.Empty:
                break

    def _make_connection(self):
        import paramiko
        cfg = self._cfg
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        kwargs = dict(
            hostname=cfg['ssh_host'],
            port=int(cfg.get('ssh_port') or 22),
            username=cfg['ssh_user'],
            timeout=30,
            banner_timeout=30,
        )
        key_path = str(cfg.get('ssh_key') or '').strip()
        if key_path:
            kwargs['key_filename'] = os.path.expanduser(key_path)
        else:
            kwargs['password'] = cfg.get('ssh_password', '')
        client.connect(**kwargs)
        return client


# ── streaming compression writer ─────────────────────────────────────────────

class StreamCompressor:
    """Wrap an output file object with incremental compression."""

    def __init__(self, f_out, fmt: str = 'none', level: Optional[int] = None):
        self._raw = f_out
        self._fmt = fmt.lower() if fmt else 'none'
        self._gz  = None
        self._bz2 = None
        self._zst = None

        if self._fmt == 'gzip':
            lvl = level if level is not None else 6
            self._gz = gzip.GzipFile(fileobj=f_out, mode='wb', compresslevel=lvl)

        elif self._fmt == 'bzip2':
            lvl = level if level is not None else 9
            self._bz2 = bz2.BZ2File(f_out, mode='w', compresslevel=lvl)

        elif self._fmt == 'zstd':
            try:
                import zstandard as zstd
                lvl = level if level is not None else 3
                cctx = zstd.ZstdCompressor(level=lvl)
                self._zst = cctx.stream_writer(f_out)
            except ImportError:
                logger.warning('zstandard not installed — streaming uncompressed')
                self._fmt = 'none'

    def write(self, data: bytes):
        if self._gz:
            self._gz.write(data)
        elif self._bz2:
            self._bz2.write(data)
        elif self._zst:
            self._zst.write(data)
        else:
            self._raw.write(data)

    def flush(self):
        if self._gz:
            self._gz.flush()
        elif self._bz2:
            pass
        elif self._zst:
            try:
                self._zst.flush()
            except Exception:
                pass
        else:
            self._raw.flush()

    def close(self):
        if self._gz:
            self._gz.close()
        elif self._bz2:
            self._bz2.close()
        elif self._zst:
            self._zst.close()
        # Don't close raw — caller owns it


# ── StreamingDumper ───────────────────────────────────────────────────────────

class StreamingDumper:
    """
    Memory-efficient parallel table dumper.

    Usage:
        sd = StreamingDumper(db_config, threads=4, comp_format='gzip')
        sd.dump(output_path, progress_cb=my_cb)
    """

    def __init__(self, db_config: dict,
                 threads: int = 4,
                 comp_format: str = 'none',
                 comp_level: Optional[int] = None):
        self.cfg         = db_config
        self.threads     = max(1, threads)
        self.comp_format = comp_format or 'none'
        self.comp_level  = comp_level
        self._cancel     = threading.Event()
        self._ssh_pool: Optional[SSHConnectionPool] = None

    def cancel(self):
        self._cancel.set()

    def dump(self, output_path: str, progress_cb: Optional[Callable] = None) -> bool:
        """
        Dump the database to output_path (possibly with compression extension).
        progress_cb(percent, message) is called periodically.
        Returns True on success.
        """
        db_type = self.cfg.get('type', '').lower()
        use_ssh = self.cfg.get('use_ssh', False)

        # Determine actual output path (add compression extension if needed)
        from compression import EXTENSIONS
        if self.comp_format and self.comp_format != 'none':
            if not output_path.endswith(EXTENSIONS.get(self.comp_format, '')):
                output_path = output_path + EXTENSIONS[self.comp_format]

        try:
            if use_ssh and self.cfg.get('ssh_host'):
                self._ssh_pool = SSHConnectionPool(self.cfg, max_size=self.threads + 2)

            if db_type == 'postgresql':
                return self._dump_pg(output_path, progress_cb)
            elif db_type == 'mysql':
                return self._dump_mysql(output_path, progress_cb)
            else:
                # Unsupported for streaming — fall through to standard dump
                if progress_cb:
                    progress_cb(0, f'Streaming not supported for {db_type}, use standard dump')
                return False
        finally:
            if self._ssh_pool:
                self._ssh_pool.close_all()

    # ── PostgreSQL streaming ──────────────────────────────────────────────────

    def _dump_pg(self, output_path: str, cb: Optional[Callable]) -> bool:
        """Stream pg_dump output through optional compression into output_path."""
        import subprocess

        if cb:
            cb(5, 'Fetching PostgreSQL table list…')

        tables = self._pg_table_list()
        if not tables:
            if cb:
                cb(10, 'No tables found — dumping full schema')
            tables = []

        if cb:
            cb(10, f'Found {len(tables)} tables, starting parallel dump (threads={self.threads})…')

        env = os.environ.copy()
        env['PGPASSWORD'] = self.cfg.get('password', '')

        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

        with open(output_path, 'wb') as raw_out:
            writer = StreamCompressor(raw_out, self.comp_format, self.comp_level)
            try:
                # Dump schema first
                if cb:
                    cb(12, 'Dumping schema…')
                schema_cmd = self._pg_cmd(schema_only=True)
                proc = subprocess.Popen(schema_cmd, env=env, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE)
                for chunk in iter(lambda: proc.stdout.read(CHUNK_SIZE), b''):
                    if self._cancel.is_set():
                        proc.terminate()
                        return False
                    writer.write(chunk)
                proc.wait()

                if not tables:
                    if cb:
                        cb(100, 'Done (schema only)')
                    writer.close()
                    return True

                # Dump data tables in parallel, collecting temp files
                if cb:
                    cb(20, f'Dumping {len(tables)} tables in parallel…')

                done = 0
                with ThreadPoolExecutor(max_workers=self.threads) as pool:
                    futs = {
                        pool.submit(self._pg_dump_table, tbl, env): tbl
                        for tbl in tables
                    }
                    for fut in as_completed(futs):
                        tbl = futs[fut]
                        if self._cancel.is_set():
                            return False
                        try:
                            data = fut.result()
                            writer.write(data)
                        except Exception as e:
                            logger.warning(f'Failed to dump table {tbl}: {e}')
                        done += 1
                        if cb:
                            pct = 20 + int(done / len(tables) * 75)
                            cb(pct, f'Dumped {done}/{len(tables)} tables…')

                writer.flush()
            finally:
                writer.close()

        if cb:
            cb(100, 'Streaming dump completed')
        return True

    def _pg_cmd(self, schema_only: bool = False, table: Optional[str] = None) -> list:
        cfg = self.cfg
        cmd = [
            'pg_dump',
            '-h', cfg.get('host', 'localhost'),
            '-p', str(int(cfg.get('port') or 5432)),
            '-U', cfg.get('user', 'postgres'),
            '--no-password',
            '--format=plain',
        ]
        if schema_only:
            cmd.append('--schema-only')
        if table:
            cmd.extend(['-t', table])
        cmd.append(cfg.get('database', 'postgres'))
        return cmd

    def _pg_dump_table(self, table: str, env: dict) -> bytes:
        """Dump a single table's data as SQL bytes."""
        import subprocess
        cmd = self._pg_cmd(table=table) + ['--data-only']
        proc = subprocess.run(cmd, env=env, capture_output=True, timeout=3600)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.decode('utf-8', errors='replace')[:300])
        return proc.stdout

    def _pg_table_list(self) -> list:
        """Return list of user table names via psycopg2 (direct) or SSH."""
        use_ssh = self.cfg.get('use_ssh', False)
        if use_ssh:
            return self._pg_tables_ssh()
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=self.cfg['host'],
                port=int(self.cfg.get('port') or 5432),
                user=self.cfg['user'],
                password=self.cfg['password'],
                dbname=self.cfg['database'],
                connect_timeout=10,
            )
            cur = conn.cursor()
            cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public'")
            tables = [r[0] for r in cur.fetchall()]
            cur.close()
            conn.close()
            return tables
        except Exception as e:
            logger.warning(f'_pg_table_list direct: {e}')
            return []

    def _pg_tables_ssh(self) -> list:
        if not self._ssh_pool:
            return []
        ssh = self._ssh_pool.acquire()
        try:
            cfg = self.cfg
            cmd = (
                f"PGPASSWORD={cfg.get('password','')} "
                f"psql -h {cfg.get('host','localhost')} "
                f"-p {int(cfg.get('port') or 5432)} "
                f"-U {cfg.get('user','postgres')} "
                f"-d {cfg.get('database','postgres')} "
                f"-t -c \"SELECT tablename FROM pg_tables WHERE schemaname='public'\""
            )
            transport = ssh.get_transport()
            chan = transport.open_session()
            chan.exec_command(cmd)
            out = b''
            while True:
                if chan.recv_ready():
                    out += chan.recv(65536)
                if chan.exit_status_ready():
                    break
                time.sleep(0.05)
            return [l.strip() for l in out.decode('utf-8', errors='replace').split('\n') if l.strip()]
        except Exception as e:
            logger.warning(f'_pg_tables_ssh: {e}')
            return []
        finally:
            self._ssh_pool.release(ssh)

    # ── MySQL streaming ───────────────────────────────────────────────────────

    def _dump_mysql(self, output_path: str, cb: Optional[Callable]) -> bool:
        """Stream mysqldump output through optional compression."""
        import subprocess

        if cb:
            cb(5, 'Starting MySQL streaming dump…')

        cfg = self.cfg
        cmd = [
            'mysqldump',
            '-h', cfg.get('host', 'localhost'),
            '-P', str(int(cfg.get('port') or 3306)),
            '-u', cfg.get('user', 'root'),
            f"-p{cfg.get('password', '')}",
            '--single-transaction', '--routines', '--triggers',
            cfg.get('database', ''),
        ]

        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

        with open(output_path, 'wb') as raw_out:
            writer = StreamCompressor(raw_out, self.comp_format, self.comp_level)
            try:
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                total_written = 0
                for chunk in iter(lambda: proc.stdout.read(CHUNK_SIZE), b''):
                    if self._cancel.is_set():
                        proc.terminate()
                        return False
                    writer.write(chunk)
                    total_written += len(chunk)
                    if cb and total_written % (10 * 1024 * 1024) == 0:
                        mb = total_written / 1_048_576
                        cb(50, f'Streamed {mb:.1f} MB…')
                proc.wait()
                if proc.returncode != 0:
                    err = proc.stderr.read().decode('utf-8', errors='replace')[:300]
                    raise RuntimeError(f'mysqldump failed: {err}')
                writer.flush()
            finally:
                writer.close()

        if cb:
            cb(100, 'MySQL streaming dump completed')
        return True
