"""
DatabaseDumper v2
─────────────────
Strategy (SSH mode, recommended):
  1. SSH connect to the server
  2. Detect native dump utility (pg_dump / mysqldump / expdp)
  3. Check free disk space on remote for the dump file
  4. Run the utility on the remote server → dump saved there
  5. SFTP-download the dump file to local path
  6. (optionally) delete the remote temp file

Direct mode (no SSH):
  - Full schema via Python drivers + system catalogs / data dictionaries
    (tables, views, sequences, FK, indexes, triggers, functions, etc.)
  - Note: SSH mode gives a true binary-compatible dump via native tools.
"""

import os
import time
import logging
import posixpath
from datetime import datetime, date, timedelta
from decimal import Decimal

import paramiko

logger = logging.getLogger(__name__)

DEFAULT_PORTS = {'postgresql': 5432, 'mysql': 3306, 'oracle': 1521}


# ── helpers ───────────────────────────────────────────────────────────────────

def _port(cfg: dict) -> int:
    raw = cfg.get('port')
    if raw is None or raw == '' or raw == 0:
        return DEFAULT_PORTS.get(cfg.get('type', '').lower(), 5432)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return DEFAULT_PORTS.get(cfg.get('type', '').lower(), 5432)


def _ssh_port(cfg: dict) -> int:
    raw = cfg.get('ssh_port')
    try:
        return int(raw) if raw else 22
    except (TypeError, ValueError):
        return 22


def _q(s) -> str:
    """POSIX single-quote a value for shell safety."""
    return "'" + str(s).replace("'", "'\\''") + "'"


def _safe_val(v) -> str:
    """Convert any Python DB value to a SQL literal, handling edge cases."""
    if v is None:
        return 'NULL'
    if isinstance(v, bool):
        return 'TRUE' if v else 'FALSE'
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if v != v:          # NaN
            return 'NULL'
        return repr(v)
    if isinstance(v, Decimal):
        return str(v)
    # datetime / date — guard against out-of-range years (BC dates etc.)
    if isinstance(v, datetime):
        try:
            return "'" + v.isoformat(sep=' ') + "'"
        except (ValueError, OverflowError):
            return 'NULL'
    if isinstance(v, date):
        try:
            return "'" + v.isoformat() + "'"
        except (ValueError, OverflowError):
            return 'NULL'
    if isinstance(v, (bytes, bytearray, memoryview)):
        if isinstance(v, memoryview):
            v = bytes(v)
        return "E'\\\\x" + v.hex() + "'"
    # timedelta (PostgreSQL interval)
    if isinstance(v, timedelta):
        total_seconds = int(v.total_seconds())
        return f"interval '{total_seconds} seconds'"
    # fallback: stringify and escape
    return "'" + str(v).replace("'", "''") + "'"


def _safe_val_mysql(v) -> str:
    """MySQL-flavoured SQL literal serializer."""
    if v is None:
        return 'NULL'
    if isinstance(v, bool):
        return '1' if v else '0'
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if v != v:
            return 'NULL'
        return repr(v)
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, (datetime, date)):
        try:
            if isinstance(v, datetime):
                return "'" + v.strftime('%Y-%m-%d %H:%M:%S') + "'"
            return "'" + v.strftime('%Y-%m-%d') + "'"
        except (ValueError, OverflowError):
            return 'NULL'
    if isinstance(v, timedelta):
        total = int(v.total_seconds())
        h, rem = divmod(abs(total), 3600)
        m, s = divmod(rem, 60)
        sign = '-' if total < 0 else ''
        return f"'{sign}{h:02d}:{m:02d}:{s:02d}'"
    if isinstance(v, (bytes, bytearray)):
        return "0x" + v.hex()
    s = str(v)
    # MySQL escaping: backslash + single-quote
    s = s.replace('\\', '\\\\').replace("'", "\\'")
    return "'" + s + "'"


class DatabaseDumper:
    def __init__(self, db_config: dict, socketio, dump_id, progress_cb,
                 cancel_check=None):
        self.cfg = db_config
        self.socketio = socketio
        self.dump_id = dump_id
        self.progress_cb = progress_cb
        self.cancel_check = cancel_check or (lambda: False)
        self._remote_filepath_actual = None
        self._local_filepath_actual  = None

    # ── progress ──────────────────────────────────────────────────────────────

    def _emit(self, percent: int, message: str, status: str = 'running'):
        logger.info("[%s] %3d%% %s", self.dump_id or '-', percent, message)
        if self.progress_cb:
            self.progress_cb(self.dump_id, {
                'status': status,
                'percent': percent,
                'message': message,
            })

    # ── SSH ───────────────────────────────────────────────────────────────────

    def _ssh_client(self) -> paramiko.SSHClient:
        cfg = self.cfg
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        kwargs = dict(
            hostname=cfg['ssh_host'],
            port=_ssh_port(cfg),
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

    def _ssh_run(self, client: paramiko.SSHClient, cmd: str, timeout: int = 3600):
        """Execute cmd on remote, wait for exit. Returns (stdout, stderr, exit_code)."""
        transport = client.get_transport()
        chan = transport.open_session()
        chan.settimeout(timeout)
        chan.exec_command(cmd)
        out_parts, err_parts = [], []
        while True:
            if chan.recv_ready():
                out_parts.append(chan.recv(65536))
            if chan.recv_stderr_ready():
                err_parts.append(chan.recv_stderr(65536))
            if chan.exit_status_ready():
                while chan.recv_ready():
                    out_parts.append(chan.recv(65536))
                while chan.recv_stderr_ready():
                    err_parts.append(chan.recv_stderr(65536))
                break
            time.sleep(0.05)
        return (
            b''.join(out_parts).decode('utf-8', errors='replace').strip(),
            b''.join(err_parts).decode('utf-8', errors='replace').strip(),
            chan.recv_exit_status(),
        )

    # ── connection test ───────────────────────────────────────────────────────

    def test_connection(self):
        try:
            use_ssh = self.cfg.get('use_ssh', False)
            db_type = self.cfg.get('type', '').lower()
            if use_ssh:
                return self._test_ssh_and_db(db_type)
            else:
                return self._test_db_direct(db_type)
        except Exception as e:
            logger.exception('test_connection')
            return False, str(e)

    def _test_ssh_and_db(self, db_type: str):
        client = self._ssh_client()
        try:
            out, err, rc = self._ssh_run(client, 'echo __SSH_OK__')
            if '__SSH_OK__' not in out:
                return False, f'SSH handshake failed: {err}'

            cfg = self.cfg
            if db_type == 'postgresql':
                cmd = (
                    f"PGPASSWORD={_q(cfg['password'])} "
                    f"psql -h {_q(cfg['host'])} -p {_port(cfg)} "
                    f"-U {_q(cfg['user'])} -d {_q(cfg['database'])} "
                    f"-c 'SELECT 1' -t -q 2>&1 | head -3"
                )
            elif db_type == 'mysql':
                cmd = (
                    f"mysql -h {_q(cfg['host'])} -P {_port(cfg)} "
                    f"-u {_q(cfg['user'])} -p{_q(cfg['password'])} "
                    f"-e 'SELECT 1' {_q(cfg['database'])} 2>&1 | head -3"
                )
            elif db_type == 'oracle':
                svc = cfg.get('service_name') or cfg.get('database', '')
                cmd = (
                    f"echo 'SELECT 1 FROM dual;' | "
                    f"sqlplus -S {_q(cfg['user'])}/{_q(cfg['password'])}"
                    f"@{_q(cfg['host'])}:{_port(cfg)}/{_q(svc)} 2>&1 | head -5"
                )
            else:
                return False, f'Unknown DB type: {db_type}'

            out, err, rc = self._ssh_run(client, cmd, timeout=20)
            combined = (out + ' ' + err).lower()
            bad = ['error', 'denied', 'refused', 'failed', 'cannot',
                   'invalid', 'no such', 'unknown', 'ora-', 'fatal']
            for w in bad:
                if w in combined:
                    return False, f'DB error via SSH: {(out + " " + err).strip()[:300]}'
            return True, f'SSH ✓  |  DB ({db_type.upper()}) ✓  |  {out[:120]}'
        finally:
            client.close()

    def _test_db_direct(self, db_type: str):
        cfg = self.cfg
        try:
            if db_type == 'postgresql':
                import psycopg2
                conn = psycopg2.connect(
                    host=cfg['host'], port=_port(cfg),
                    user=cfg['user'], password=cfg['password'],
                    dbname=cfg['database'], connect_timeout=10,
                )
                conn.close()
            elif db_type == 'mysql':
                import pymysql
                conn = pymysql.connect(
                    host=cfg['host'], port=_port(cfg),
                    user=cfg['user'], password=cfg['password'],
                    database=cfg['database'], connect_timeout=10,
                )
                conn.close()
            elif db_type == 'oracle':
                import cx_Oracle
                dsn = cx_Oracle.makedsn(
                    cfg['host'], _port(cfg),
                    service_name=cfg.get('service_name') or cfg.get('database'),
                )
                conn = cx_Oracle.connect(cfg['user'], cfg['password'], dsn)
                conn.close()
            else:
                return False, f'Unknown DB type: {db_type}'
            return True, 'Direct connection successful'
        except Exception as e:
            return False, str(e)

    # ── main entry point ──────────────────────────────────────────────────────

    def dump(self, local_filepath: str) -> bool:
        db_type = self.cfg.get('type', '').lower()
        use_ssh = self.cfg.get('use_ssh', False)
        self._emit(3, f'Starting {db_type.upper()} dump…')
        try:
            if use_ssh:
                return self._dump_ssh(db_type, local_filepath)
            else:
                return self._dump_direct(db_type, local_filepath)
        except Exception as e:
            logger.exception('dump')
            self._emit(0, str(e), 'error')
            return False

    # ══════════════════════════════════════════════════════════════════════════
    #  SSH MODE — native tools on the remote server
    # ══════════════════════════════════════════════════════════════════════════

    def _dump_ssh(self, db_type: str, local_filepath: str) -> bool:
        cfg = self.cfg
        self._emit(5, 'Connecting via SSH…')
        client = self._ssh_client()
        try:
            self._emit(8, 'SSH connected')

            remote_tmp_dir = (cfg.get('remote_tmp_dir') or '').strip() or '/tmp'
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            db_name = cfg.get('database', 'dump')
            ext = 'dmp' if db_type == 'oracle' else 'sql'
            remote_filename = f"dbdump_{db_name}_{timestamp}.{ext}"
            remote_filepath = posixpath.join(remote_tmp_dir, remote_filename)

            if db_type == 'postgresql':
                ok = self._pg_dump_ssh(client, remote_filepath)
            elif db_type == 'mysql':
                ok = self._mysql_dump_ssh(client, remote_filepath)
            elif db_type == 'oracle':
                ok = self._oracle_dump_ssh(client, remote_filepath)
            else:
                self._emit(0, f'Unsupported DB type: {db_type}', 'error')
                return False

            if not ok:
                return False

            # Use the actual remote path (may have .gz extension added)
            actual_remote = self._remote_filepath_actual or remote_filepath
            ok = self._sftp_download(client, actual_remote, local_filepath)

            if ok and cfg.get('delete_remote_after', True):
                self._emit(98, 'Removing remote temp file…')
                self._ssh_run(client, f'rm -f {_q(actual_remote)}')

            return ok
        finally:
            client.close()

    # ── remote disk space check ───────────────────────────────────────────────

    def _check_remote_space(self, client, remote_dir: str, required_bytes: int):
        cmd = f"df -Pk {_q(remote_dir)} 2>/dev/null | awk 'NR==2{{print $4}}'"
        out, _, _ = self._ssh_run(client, cmd, timeout=15)
        try:
            free_kb = int(out.strip())
        except (ValueError, TypeError):
            return True, 'Could not determine remote disk space — proceeding'

        free_bytes = free_kb * 1024
        needed_bytes = int(required_bytes * 1.25)
        free_gb   = free_bytes   / 1_073_741_824
        needed_gb = needed_bytes / 1_073_741_824
        db_gb     = required_bytes / 1_073_741_824

        if free_bytes < needed_bytes:
            return False, (
                f'Not enough remote disk space in {remote_dir}: '
                f'need ~{needed_gb:.2f} GB (DB {db_gb:.2f} GB + 25% margin), '
                f'only {free_gb:.2f} GB free'
            )
        return True, (
            f'Remote disk OK: {free_gb:.2f} GB free, need ~{needed_gb:.2f} GB'
        )

    # ── PostgreSQL — pg_dump ──────────────────────────────────────────────────

    def _pg_dump_ssh(self, client, remote_path: str) -> bool:
        cfg = self.cfg
        self._emit(10, 'Checking pg_dump on remote server…')

        out, _, rc = self._ssh_run(client, 'which pg_dump || command -v pg_dump')
        if rc != 0 or not out.strip():
            self._emit(0, 'pg_dump not found on remote server. Install postgresql-client.', 'error')
            return False
        pg_dump_bin = out.strip().split('\n')[0]
        self._emit(12, f'pg_dump found: {pg_dump_bin}')

        # Estimate size
        self._emit(14, 'Estimating database size…')
        size_cmd = (
            f"PGPASSWORD={_q(cfg['password'])} "
            f"psql -h {_q(cfg['host'])} -p {_port(cfg)} "
            f"-U {_q(cfg['user'])} -d {_q(cfg['database'])} -t -q "
            f"-c \"SELECT pg_database_size(current_database())\" 2>/dev/null"
        )
        out, _, _ = self._ssh_run(client, size_cmd, timeout=30)
        db_size_bytes = 0
        try:
            db_size_bytes = int(out.strip())
        except (ValueError, TypeError):
            pass

        remote_dir = posixpath.dirname(remote_path)
        if db_size_bytes > 0:
            ok, msg = self._check_remote_space(client, remote_dir, db_size_bytes)
            if not ok:
                self._emit(0, msg, 'error')
                return False
            self._emit(16, msg)

        # Build pg_dump command
        self._emit(18, 'Building pg_dump command…')
        dump_fmt = cfg.get('dump_format', 'plain')
        # Map format to extension
        ext_map = {'plain': '.sql', 'custom': '.dump', 'directory': '', 'tar': '.tar'}
        new_ext = ext_map.get(dump_fmt, '.sql')
        # For directory format, remote_path IS the directory name
        if dump_fmt == 'directory':
            remote_path = remote_path.replace('.sql', '')
        elif new_ext != '.sql':
            remote_path = remote_path.replace('.sql', new_ext)

        parts = [
            f"PGPASSWORD={_q(cfg['password'])}",
            pg_dump_bin,
            f"-h {_q(cfg['host'])}",
            f"-p {_port(cfg)}",
            f"-U {_q(cfg['user'])}",
            f"-d {_q(cfg['database'])}",
            f"--format={dump_fmt}",
            '--no-password',
        ]

        dump_mode = cfg.get('dump_mode', 'full')
        if dump_mode == 'schema_only':
            parts.append('--schema-only')
        elif dump_mode == 'data_only':
            parts.append('--data-only')
        # else full: pg_dump defaults include tables, views, sequences,
        # functions, triggers, indexes, constraints — everything

        if dump_mode != 'data_only':
            if cfg.get('no_owner', False):
                parts.append('--no-owner')
            if cfg.get('no_acl', False):
                parts.append('--no-acl')
            if cfg.get('clean', True):
                parts.append('--clean')
            if cfg.get('if_exists', True):
                parts.append('--if-exists')
            # Include/exclude schemas
            for s in cfg.get('include_schemas', []):
                parts.append(f'--schema={_q(s)}')
            for s in cfg.get('exclude_schemas', []):
                parts.append(f'--exclude-schema={_q(s)}')

        # Include/exclude tables
        for t in cfg.get('include_tables', []):
            parts.append(f'--table={_q(t)}')
        for t in cfg.get('exclude_tables', []):
            parts.append(f'--exclude-table={_q(t)}')

        compress = int(cfg.get('compress_level', 0) or 0)
        if dump_fmt == 'directory':
            # directory format uses -f, no redirect needed
            parts.append(f'-f {_q(remote_path)}')
        elif compress > 0 and dump_fmt == 'plain':
            remote_path += '.gz'
            parts.append(f'| gzip -{compress} > {_q(remote_path)}')
        else:
            parts.append(f'-f {_q(remote_path)}')

        cmd = ' '.join(parts)
        self._emit(20, f'Running pg_dump (format={dump_fmt}) on remote server…')
        out, err, rc = self._ssh_run(client, cmd, timeout=7200)

        if rc != 0:
            self._emit(0, f'pg_dump failed (exit {rc}): {err[:500]}', 'error')
            return False

        if dump_fmt == 'directory':
            out, _, _ = self._ssh_run(client, f'du -sb {_q(remote_path)} 2>/dev/null | cut -f1 || echo 0')
        else:
            out, _, _ = self._ssh_run(client, f'stat -c%s {_q(remote_path)} 2>/dev/null || echo 0')
        file_size = int((out.strip() or '0').split()[0])
        if file_size == 0:
            self._emit(0, 'Dump file is empty on remote server', 'error')
            return False

        self._emit(60, f'Dump ready on server: {file_size/1_048_576:.1f} MB — downloading via SFTP…')
        self._remote_filepath_actual = remote_path
        return True

    # ── MySQL — mysqldump ─────────────────────────────────────────────────────

    def _mysql_dump_ssh(self, client, remote_path: str) -> bool:
        cfg = self.cfg
        self._emit(10, 'Checking mysqldump on remote server…')

        out, _, rc = self._ssh_run(client, 'which mysqldump || command -v mysqldump')
        if rc != 0 or not out.strip():
            self._emit(0, 'mysqldump not found on remote server.', 'error')
            return False
        dump_bin = out.strip().split('\n')[0]
        self._emit(12, f'mysqldump found: {dump_bin}')

        # Estimate size
        self._emit(14, 'Estimating database size…')
        size_cmd = (
            f"mysql -h {_q(cfg['host'])} -P {_port(cfg)} "
            f"-u {_q(cfg['user'])} -p{_q(cfg['password'])} "
            f"-N -e \"SELECT SUM(data_length+index_length) "
            f"FROM information_schema.tables "
            f"WHERE table_schema={_q(cfg['database'])}\" 2>/dev/null"
        )
        out, _, _ = self._ssh_run(client, size_cmd, timeout=30)
        db_size_bytes = 0
        try:
            db_size_bytes = int(out.strip())
        except (ValueError, TypeError):
            pass

        remote_dir = posixpath.dirname(remote_path)
        if db_size_bytes > 0:
            ok, msg = self._check_remote_space(client, remote_dir, db_size_bytes)
            if not ok:
                self._emit(0, msg, 'error')
                return False
            self._emit(16, msg)

        # Build mysqldump command
        self._emit(18, 'Building mysqldump command…')
        parts = [
            dump_bin,
            f"-h {_q(cfg['host'])}",
            f"-P {_port(cfg)}",
            f"-u {_q(cfg['user'])}",
            f"-p{_q(cfg['password'])}",
            '--single-transaction',   # consistent InnoDB snapshot
            '--routines',             # stored procedures + functions
            '--triggers',             # triggers
            '--events',               # scheduled events
            '--comments',
            '--hex-blob',
            '--default-character-set=utf8mb4',
        ]

        dump_mode = cfg.get('dump_mode', 'full')
        if dump_mode == 'schema_only':
            parts.append('--no-data')
        elif dump_mode == 'data_only':
            parts.append('--no-create-info')
            parts.append('--no-create-db')

        if cfg.get('add_drop_table', True):
            parts.append('--add-drop-table')
        if cfg.get('no_locks', False):
            parts.append('--skip-lock-tables')

        include_tables = cfg.get('include_tables', [])
        exclude_tables = cfg.get('exclude_tables', [])
        parts.append(_q(cfg['database']))
        if include_tables:
            parts += [_q(t) for t in include_tables]
        if exclude_tables:
            parts += [f"--ignore-table={_q(cfg['database'])}.{_q(t)}"
                      for t in exclude_tables]

        compress = int(cfg.get('compress_level', 0) or 0)
        if compress > 0:
            remote_path += '.gz'
            parts.append(f'| gzip -{compress} > {_q(remote_path)}')
        else:
            parts.append(f'> {_q(remote_path)}')

        cmd = ' '.join(parts)
        self._emit(20, 'Running mysqldump on remote server…')
        out, err, rc = self._ssh_run(client, cmd, timeout=7200)

        if rc != 0:
            self._emit(0, f'mysqldump failed (exit {rc}): {err[:500]}', 'error')
            return False

        out, _, _ = self._ssh_run(client, f'stat -c%s {_q(remote_path)} 2>/dev/null || echo 0')
        file_size = int((out.strip() or '0').split()[0])
        if file_size == 0:
            self._emit(0, 'Dump file is empty on remote server', 'error')
            return False

        self._emit(60, f'Dump ready on server: {file_size/1_048_576:.1f} MB — downloading via SFTP…')
        self._remote_filepath_actual = remote_path
        return True

    # ── Oracle — expdp / exp ──────────────────────────────────────────────────

    def _oracle_dump_ssh(self, client, remote_path: str) -> bool:
        cfg = self.cfg
        self._emit(10, 'Checking Oracle export tools on remote server…')

        out, _, rc = self._ssh_run(client, 'which expdp || command -v expdp')
        use_expdp = rc == 0 and out.strip()
        if use_expdp:
            dump_bin = out.strip().split('\n')[0]
            self._emit(12, f'expdp found: {dump_bin}')
        else:
            out, _, rc = self._ssh_run(client, 'which exp || command -v exp')
            if rc != 0 or not out.strip():
                self._emit(0, 'Neither expdp nor exp found on remote server.', 'error')
                return False
            dump_bin = out.strip().split('\n')[0]
            self._emit(12, f'exp (classic) found: {dump_bin}')

        # Estimate size
        self._emit(14, 'Estimating Oracle schema size…')
        svc = cfg.get('service_name') or cfg.get('database', '')
        conn_str = f"{cfg['user']}/{cfg['password']}@{cfg['host']}:{_port(cfg)}/{svc}"
        size_sql = "SELECT SUM(bytes) FROM user_segments;"
        size_cmd = f"echo {_q(size_sql)} | sqlplus -S {_q(conn_str)} 2>/dev/null | grep -E '^[0-9]'"
        out, _, _ = self._ssh_run(client, size_cmd, timeout=30)
        db_size_bytes = 0
        try:
            db_size_bytes = int(out.strip().split('\n')[0])
        except (ValueError, TypeError):
            pass

        remote_dir = posixpath.dirname(remote_path)
        if db_size_bytes > 0:
            ok, msg = self._check_remote_space(client, remote_dir, db_size_bytes)
            if not ok:
                self._emit(0, msg, 'error')
                return False
            self._emit(16, msg)

        dump_mode = cfg.get('dump_mode', 'full')
        dump_filename = posixpath.basename(remote_path)

        if use_expdp:
            # expdp needs a DIRECTORY object in Oracle pointing to remote_dir
            dir_obj = 'DBDUMP_TMP_DIR'
            setup_sql = (
                f"CREATE OR REPLACE DIRECTORY {dir_obj} AS '{remote_dir}';\n"
                f"GRANT READ, WRITE ON DIRECTORY {dir_obj} TO {cfg['user']};\n"
                f"EXIT;\n"
            )
            self._emit(18, 'Creating Oracle DIRECTORY object…')
            self._ssh_run(client,
                f"echo {_q(setup_sql)} | sqlplus -S {_q(conn_str)}", timeout=30)

            content_flag = {
                'schema_only': 'CONTENT=METADATA_ONLY',
                'data_only':   'CONTENT=DATA_ONLY',
            }.get(dump_mode, '')

            schemas = cfg.get('include_schemas', [cfg['user']])
            cmd = (
                f"{dump_bin} userid={_q(conn_str)} "
                f"directory={dir_obj} dumpfile={_q(dump_filename)} "
                f"logfile=dbdump_expdp_{posixpath.splitext(dump_filename)[0]}.log "
                f"schemas={','.join(schemas)} "
                f"{content_flag}"
            )
        else:
            rows_flag = 'N' if dump_mode == 'schema_only' else 'Y'
            cmd = (
                f"{dump_bin} userid={_q(conn_str)} "
                f"file={_q(remote_path)} "
                f"log={_q(remote_path + '.log')} "
                f"rows={rows_flag} owner={_q(cfg['user'])}"
            )

        self._emit(20, 'Running Oracle export on remote server (may take a while)…')
        out, err, rc = self._ssh_run(client, cmd, timeout=14400)

        # expdp: rc=0 success, rc=5 warnings — both OK
        if rc not in (0, 5):
            self._emit(0, f'Oracle export failed (exit {rc}): {err[:500]}', 'error')
            return False

        out, _, _ = self._ssh_run(client, f'stat -c%s {_q(remote_path)} 2>/dev/null || echo 0')
        file_size = int((out.strip() or '0').split()[0])
        if file_size == 0:
            self._emit(0, 'Dump file is empty on remote server', 'error')
            return False

        self._emit(60, f'Dump ready on server: {file_size/1_048_576:.1f} MB — downloading via SFTP…')
        self._remote_filepath_actual = remote_path
        return True

    # ── SFTP download ─────────────────────────────────────────────────────────

    def _sftp_download(self, client, remote_path: str, local_path: str) -> bool:
        self._emit(61, 'SFTP: opening connection…')
        try:
            sftp = client.open_sftp()

            # Detect if remote_path is a directory (pg_dump --format=directory)
            try:
                rstat = sftp.stat(remote_path)
                import stat as stat_mod
                is_dir = stat_mod.S_ISDIR(rstat.st_mode)
            except Exception:
                is_dir = False

            if is_dir:
                return self._sftp_download_dir(sftp, remote_path, local_path)

            # Single file download
            try:
                total_size = sftp.stat(remote_path).st_size
            except Exception:
                total_size = 0

            if remote_path.endswith('.gz') and not local_path.endswith('.gz'):
                local_path += '.gz'

            os.makedirs(os.path.dirname(os.path.abspath(local_path)), exist_ok=True)
            start_time = time.time()

            def _cb(done, total):
                elapsed = max(time.time() - start_time, 0.001)
                speed_mb = (done / elapsed) / 1_048_576
                sz = total or total_size
                pct = (61 + int((done / sz) * 35)) if sz else 70
                self._emit(min(96, pct),
                    f'Downloading: {done/1_048_576:.1f} / {sz/1_048_576:.1f} MB  ({speed_mb:.1f} MB/s)')

            sftp.get(remote_path, local_path, callback=_cb)
            sftp.close()
            self._local_filepath_actual = local_path
            size = os.path.getsize(local_path)
            self._emit(97, f'Download complete: {size/1_048_576:.1f} MB  →  {local_path}')
            return True
        except Exception as e:
            logger.exception('SFTP download failed')
            self._emit(0, f'SFTP download failed: {e}', 'error')
            return False

    def _sftp_download_dir(self, sftp, remote_dir: str, local_base: str) -> bool:
        """Recursively download a remote directory (for pg_dump --format=directory)."""
        import stat as stat_mod
        os.makedirs(local_base, exist_ok=True)
        total_files = 0
        downloaded  = 0

        def _count(path):
            nonlocal total_files
            for entry in sftp.listdir_attr(path):
                if stat_mod.S_ISDIR(entry.st_mode):
                    _count(posixpath.join(path, entry.filename))
                else:
                    total_files += 1

        def _download(rpath, lpath):
            nonlocal downloaded
            os.makedirs(lpath, exist_ok=True)
            for entry in sftp.listdir_attr(rpath):
                rfile = posixpath.join(rpath, entry.filename)
                lfile = os.path.join(lpath, entry.filename)
                if stat_mod.S_ISDIR(entry.st_mode):
                    _download(rfile, lfile)
                else:
                    sftp.get(rfile, lfile)
                    downloaded += 1
                    pct = 61 + int((downloaded / max(total_files, 1)) * 35)
                    self._emit(min(96, pct),
                        f'Downloading directory: {downloaded}/{total_files} files')

        try:
            _count(remote_dir)
            _download(remote_dir, local_base)
            sftp.close()
            self._local_filepath_actual = local_base
            self._emit(97, f'Directory download complete: {downloaded} files  →  {local_base}')
            return True
        except Exception as e:
            logger.exception('SFTP directory download failed')
            self._emit(0, f'SFTP directory download failed: {e}', 'error')
            return False

    # ══════════════════════════════════════════════════════════════════════════
    #  DIRECT MODE — Python drivers, full schema via system catalogs
    # ══════════════════════════════════════════════════════════════════════════

    def _dump_direct(self, db_type: str, filepath: str) -> bool:
        try:
            if db_type == 'postgresql':
                return self._pg_dump_direct(filepath)
            elif db_type == 'mysql':
                return self._mysql_dump_direct(filepath)
            elif db_type == 'oracle':
                return self._oracle_dump_direct(filepath)
            else:
                self._emit(0, f'Unsupported DB type: {db_type}', 'error')
                return False
        except Exception as e:
            self._emit(0, str(e), 'error')
            logger.exception('_dump_direct')
            return False

    # ── PostgreSQL direct — full schema via catalogs ───────────────────────────

    def _pg_dump_direct(self, filepath: str) -> bool:
        import psycopg2
        cfg = self.cfg
        conn = psycopg2.connect(
            host=cfg['host'], port=_port(cfg),
            user=cfg['user'], password=cfg['password'],
            dbname=cfg['database'],
        )
        self._emit(10, 'Connected (direct). Reading schema…')
        dump_mode = cfg.get('dump_mode', 'full')
        no_data   = dump_mode == 'schema_only'
        no_schema = dump_mode == 'data_only'
        incl = set(cfg.get('include_tables', []))
        excl = set(cfg.get('exclude_tables', []))

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"-- PostgreSQL dump (direct Python): {cfg['database']}\n")
            f.write(f"-- Generated: {datetime.now().isoformat()}\n")
            f.write("SET client_encoding = 'UTF8';\nSET standard_conforming_strings = on;\n\n")

            cur = conn.cursor()

            if not no_schema:
                self._emit(12, 'Extensions…')
                cur.execute("SELECT extname FROM pg_extension WHERE extname<>'plpgsql' ORDER BY extname")
                for (ext,) in cur.fetchall():
                    f.write(f'CREATE EXTENSION IF NOT EXISTS "{ext}";\n')
                f.write('\n')

                self._emit(13, 'Schemas…')
                cur.execute("""
                    SELECT nspname FROM pg_namespace
                    WHERE nspname NOT IN ('pg_catalog','information_schema','pg_toast')
                      AND nspname NOT LIKE 'pg_temp_%' AND nspname <> 'public'
                    ORDER BY nspname
                """)
                for (ns,) in cur.fetchall():
                    f.write(f'CREATE SCHEMA IF NOT EXISTS "{ns}";\n')
                f.write('\n')

                self._emit(14, 'Enum types…')
                cur.execute("""
                    SELECT n.nspname, t.typname,
                           array_agg(e.enumlabel ORDER BY e.enumsortorder)
                    FROM pg_type t
                    JOIN pg_enum e ON e.enumtypid=t.oid
                    JOIN pg_namespace n ON n.oid=t.typnamespace
                    GROUP BY n.nspname, t.typname ORDER BY n.nspname, t.typname
                """)
                for ns, typname, labels in cur.fetchall():
                    vals = ', '.join(f"'{l}'" for l in labels)
                    f.write(f'CREATE TYPE "{ns}"."{typname}" AS ENUM ({vals});\n')
                f.write('\n')

                self._emit(15, 'Sequences…')
                cur.execute("""
                    SELECT sequence_schema, sequence_name,
                           start_value, minimum_value, maximum_value,
                           increment, cycle_option
                    FROM information_schema.sequences
                    ORDER BY sequence_schema, sequence_name
                """)
                for ns, name, start, minv, maxv, inc, cycle in cur.fetchall():
                    f.write(
                        f'CREATE SEQUENCE IF NOT EXISTS "{ns}"."{name}"\n'
                        f'  INCREMENT BY {inc} MINVALUE {minv} MAXVALUE {maxv}\n'
                        f'  START {start} {"CYCLE" if cycle=="YES" else "NO CYCLE"};\n\n'
                    )

            # Tables
            self._emit(18, 'Fetching table list…')
            cur.execute("""
                SELECT table_schema, table_name FROM information_schema.tables
                WHERE table_type='BASE TABLE'
                  AND table_schema NOT IN ('pg_catalog','information_schema')
                ORDER BY table_schema, table_name
            """)
            all_tables = cur.fetchall()
            tables = [(s,t) for s,t in all_tables
                      if (not incl or t in incl) and t not in excl]
            total = len(tables)

            for idx, (schema, table) in enumerate(tables):
                if self.cancel_check():
                    return False
                pct = 20 + int((idx / max(total, 1)) * (55 if no_data else 45))
                self._emit(pct, f'Table {idx+1}/{total}: "{schema}"."{table}"')

                if not no_schema:
                    cur.execute("""
                        SELECT column_name, data_type, character_maximum_length,
                               numeric_precision, numeric_scale,
                               is_nullable, column_default, udt_name
                        FROM information_schema.columns
                        WHERE table_schema=%s AND table_name=%s
                        ORDER BY ordinal_position
                    """, (schema, table))
                    cols = cur.fetchall()
                    f.write(f'\n-- Table: "{schema}"."{table}"\n')
                    f.write(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE;\n')
                    f.write(f'CREATE TABLE "{schema}"."{table}" (\n')
                    defs = []
                    for name, dtype, clen, nprec, nscale, nullable, default, udt in cols:
                        if dtype == 'USER-DEFINED':
                            dtype = udt
                        d = f'    "{name}" {dtype}'
                        if clen:
                            d += f'({clen})'
                        elif nprec and dtype in ('numeric','decimal'):
                            d += f'({nprec},{nscale or 0})'
                        if default is not None:
                            d += f' DEFAULT {default}'
                        if nullable == 'NO':
                            d += ' NOT NULL'
                        defs.append(d)
                    f.write(',\n'.join(defs) + '\n);\n')

                if not no_data:
                    cur.execute(f'SELECT * FROM "{schema}"."{table}"')
                    col_names = [d[0] for d in cur.description]
                    cols_str = ', '.join(f'"{c}"' for c in col_names)
                    chunk = cur.fetchmany(500)
                    while chunk:
                        rows_sql = []
                        for row in chunk:
                            rows_sql.append('(' + ', '.join(_safe_val(v) for v in row) + ')')
                        f.write(f'\nINSERT INTO "{schema}"."{table}" ({cols_str}) VALUES\n')
                        f.write(',\n'.join(rows_sql) + ';\n')
                        chunk = cur.fetchmany(500)

            if not no_schema:
                self._emit(70, 'Primary keys & unique constraints…')
                cur.execute("""
                    SELECT tc.table_schema, tc.table_name, tc.constraint_name,
                           tc.constraint_type,
                           string_agg(kcu.column_name,',' ORDER BY kcu.ordinal_position)
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON kcu.constraint_name=tc.constraint_name
                     AND kcu.table_schema=tc.table_schema
                    WHERE tc.constraint_type IN ('PRIMARY KEY','UNIQUE')
                      AND tc.table_schema NOT IN ('pg_catalog','information_schema')
                    GROUP BY tc.table_schema,tc.table_name,tc.constraint_name,tc.constraint_type
                """)
                f.write('\n-- Primary Keys & Unique Constraints\n')
                for ts, tn, cn, ct, ca in cur.fetchall():
                    cols_q = ', '.join(f'"{c}"' for c in ca.split(','))
                    f.write(f'ALTER TABLE "{ts}"."{tn}" ADD CONSTRAINT "{cn}" {ct} ({cols_q});\n')

                self._emit(75, 'Foreign keys…')
                cur.execute("""
                    SELECT tc.table_schema, tc.table_name, tc.constraint_name,
                           string_agg(kcu.column_name,',' ORDER BY kcu.ordinal_position),
                           ccu.table_schema, ccu.table_name,
                           string_agg(ccu.column_name,',' ORDER BY kcu.ordinal_position),
                           rc.delete_rule, rc.update_rule
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON kcu.constraint_name=tc.constraint_name AND kcu.table_schema=tc.table_schema
                    JOIN information_schema.referential_constraints rc
                      ON rc.constraint_name=tc.constraint_name
                    JOIN information_schema.constraint_column_usage ccu
                      ON ccu.constraint_name=tc.constraint_name
                    WHERE tc.constraint_type='FOREIGN KEY'
                    GROUP BY tc.table_schema,tc.table_name,tc.constraint_name,
                             ccu.table_schema,ccu.table_name,rc.delete_rule,rc.update_rule
                """)
                f.write('\n-- Foreign Keys\n')
                for ts,tn,cn,ca,rts,rtn,rca,dr,ur in cur.fetchall():
                    cols_q  = ', '.join(f'"{c}"' for c in ca.split(','))
                    rcols_q = ', '.join(f'"{c}"' for c in rca.split(','))
                    f.write(
                        f'ALTER TABLE "{ts}"."{tn}" ADD CONSTRAINT "{cn}" '
                        f'FOREIGN KEY ({cols_q}) REFERENCES "{rts}"."{rtn}" ({rcols_q}) '
                        f'ON DELETE {dr} ON UPDATE {ur};\n'
                    )

                self._emit(80, 'Indexes…')
                cur.execute("""
                    SELECT indexdef FROM pg_indexes
                    WHERE schemaname NOT IN ('pg_catalog','information_schema')
                      AND indexname NOT IN (
                          SELECT constraint_name FROM information_schema.table_constraints
                          WHERE constraint_type IN ('PRIMARY KEY','UNIQUE'))
                    ORDER BY schemaname, tablename
                """)
                f.write('\n-- Indexes\n')
                for (idef,) in cur.fetchall():
                    f.write(idef + ';\n')

                self._emit(84, 'Views…')
                cur.execute("""
                    SELECT table_schema, table_name, view_definition
                    FROM information_schema.views
                    WHERE table_schema NOT IN ('pg_catalog','information_schema')
                    ORDER BY table_schema, table_name
                """)
                f.write('\n-- Views\n')
                for vs, vn, vdef in cur.fetchall():
                    f.write(f'CREATE OR REPLACE VIEW "{vs}"."{vn}" AS\n{vdef};\n\n')

                self._emit(88, 'Functions & procedures…')
                cur.execute("""
                    SELECT n.nspname, p.proname, pg_get_functiondef(p.oid)
                    FROM pg_proc p
                    JOIN pg_namespace n ON n.oid=p.pronamespace
                    WHERE n.nspname NOT IN ('pg_catalog','information_schema')
                    ORDER BY n.nspname, p.proname
                """)
                f.write('\n-- Functions\n')
                for ns, fn, fdef in cur.fetchall():
                    f.write(fdef + ';\n\n')

                self._emit(93, 'Triggers…')
                cur.execute("""
                    SELECT trigger_schema, trigger_name,
                           event_object_schema, event_object_table,
                           event_manipulation, action_timing,
                           action_statement, action_orientation
                    FROM information_schema.triggers
                    WHERE trigger_schema NOT IN ('pg_catalog','information_schema')
                    ORDER BY trigger_schema, trigger_name
                """)
                f.write('\n-- Triggers\n')
                for ts,tname,es,et,event,timing,stmt,orient in cur.fetchall():
                    f.write(
                        f'CREATE TRIGGER "{tname}" {timing} {event}\n'
                        f'ON "{es}"."{et}" FOR EACH {orient}\n{stmt};\n\n'
                    )

        cur.close(); conn.close()
        self._emit(99, 'Closing connection…')
        return True

    # ── MySQL direct — full schema ─────────────────────────────────────────────

    def _mysql_dump_direct(self, filepath: str) -> bool:
        import pymysql
        cfg = self.cfg
        conn = pymysql.connect(
            host=cfg['host'], port=_port(cfg),
            user=cfg['user'], password=cfg['password'],
            database=cfg['database'], charset='utf8mb4',
        )
        self._emit(10, 'Connected (direct). Reading schema…')
        dump_mode = cfg.get('dump_mode', 'full')
        no_data   = dump_mode == 'schema_only'
        no_schema = dump_mode == 'data_only'
        incl = set(cfg.get('include_tables', []))
        excl = set(cfg.get('exclude_tables', []))

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"-- MySQL dump (direct Python): {cfg['database']}\n")
            f.write(f"-- Generated: {datetime.now().isoformat()}\n")
            f.write("SET NAMES utf8mb4;\nSET FOREIGN_KEY_CHECKS=0;\n\n")

            cur = conn.cursor()

            if not no_schema:
                self._emit(12, 'Views…')
                cur.execute("SHOW FULL TABLES WHERE Table_type='VIEW'")
                for (vname, _) in cur.fetchall():
                    cur.execute(f"SHOW CREATE VIEW `{vname}`")
                    row = cur.fetchone()
                    f.write(f"DROP VIEW IF EXISTS `{vname}`;\n{row[1]};\n\n")

                self._emit(15, 'Routines (procedures & functions)…')
                for rtype in ('PROCEDURE', 'FUNCTION'):
                    cur.execute(f"SHOW {rtype} STATUS WHERE Db=%s", (cfg['database'],))
                    for r in cur.fetchall():
                        rname = r[1]
                        cur.execute(f"SHOW CREATE {rtype} `{rname}`")
                        row = cur.fetchone()
                        body = row[2]
                        f.write(f"DROP {rtype} IF EXISTS `{rname}`;\nDELIMITER $$\n{body}$$\nDELIMITER ;\n\n")

                self._emit(17, 'Events…')
                cur.execute("SHOW EVENTS")
                for ev in cur.fetchall():
                    evname = ev[1]
                    cur.execute(f"SHOW CREATE EVENT `{evname}`")
                    row = cur.fetchone()
                    f.write(f"DROP EVENT IF EXISTS `{evname}`;\nDELIMITER $$\n{row[3]}$$\nDELIMITER ;\n\n")

            # Tables
            self._emit(20, 'Fetching table list…')
            cur.execute("SHOW FULL TABLES WHERE Table_type='BASE TABLE'")
            all_tables = [r[0] for r in cur.fetchall()]
            tables = [t for t in all_tables
                      if (not incl or t in incl) and t not in excl]
            total = len(tables)

            for idx, table in enumerate(tables):
                if self.cancel_check():
                    return False
                pct = 22 + int((idx / max(total, 1)) * 70)
                self._emit(pct, f'Table {idx+1}/{total}: {table}')

                if not no_schema:
                    cur.execute(f"SHOW CREATE TABLE `{table}`")
                    row = cur.fetchone()
                    f.write(f"\n-- Table: {table}\nDROP TABLE IF EXISTS `{table}`;\n{row[1]};\n")
                    # Triggers
                    cur.execute("SHOW TRIGGERS LIKE %s", (table,))
                    for tr in cur.fetchall():
                        f.write(
                            f"\nDELIMITER $$\n"
                            f"CREATE TRIGGER `{tr[0]}` {tr[4]} {tr[1]} ON `{tr[2]}` FOR EACH ROW\n"
                            f"{tr[3]}$$\nDELIMITER ;\n"
                        )

                if not no_data:
                    cur.execute(f"SELECT * FROM `{table}`")
                    col_names = [d[0] for d in cur.description]
                    cols_str = ', '.join(f'`{c}`' for c in col_names)
                    chunk = cur.fetchmany(500)
                    while chunk:
                        rows_sql = ['(' + ', '.join(_safe_val_mysql(v) for v in row) + ')'
                                    for row in chunk]
                        f.write(f"\nINSERT INTO `{table}` ({cols_str}) VALUES\n")
                        f.write(',\n'.join(rows_sql) + ';\n')
                        chunk = cur.fetchmany(500)

            f.write('\nSET FOREIGN_KEY_CHECKS=1;\n')

        cur.close(); conn.close()
        self._emit(99, 'Closing connection…')
        return True

    # ── Oracle direct — full schema ────────────────────────────────────────────

    def _oracle_dump_direct(self, filepath: str) -> bool:
        import cx_Oracle
        cfg = self.cfg
        dsn = cx_Oracle.makedsn(
            cfg['host'], _port(cfg),
            service_name=cfg.get('service_name') or cfg.get('database'),
        )
        conn = cx_Oracle.connect(cfg['user'], cfg['password'], dsn)
        self._emit(10, 'Connected (direct). Reading schema…')
        dump_mode = cfg.get('dump_mode', 'full')
        no_data   = dump_mode == 'schema_only'
        no_schema = dump_mode == 'data_only'
        incl = set(cfg.get('include_tables', []))
        excl = set(cfg.get('exclude_tables', []))

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"-- Oracle dump (direct Python): {cfg.get('database','')}\n")
            f.write(f"-- Generated: {datetime.now().isoformat()}\n\n")

            cur = conn.cursor()

            if not no_schema:
                self._emit(12, 'Sequences…')
                cur.execute("""
                    SELECT sequence_name,min_value,max_value,
                           increment_by,cycle_flag,cache_size,last_number
                    FROM user_sequences ORDER BY sequence_name
                """)
                for sn,minv,maxv,inc,cyc,cache,last in cur.fetchall():
                    f.write(
                        f'CREATE SEQUENCE "{sn}"\n'
                        f'  MINVALUE {minv} MAXVALUE {maxv}\n'
                        f'  INCREMENT BY {inc} START WITH {last}\n'
                        f'  CACHE {cache} {"CYCLE" if cyc=="Y" else "NOCYCLE"};\n\n'
                    )

            # Tables
            self._emit(16, 'Fetching table list…')
            cur.execute("SELECT table_name FROM user_tables ORDER BY table_name")
            all_tables = [r[0] for r in cur.fetchall()]
            tables = [t for t in all_tables
                      if (not incl or t in incl) and t not in excl]
            total = len(tables)

            for idx, table in enumerate(tables):
                pct = 18 + int((idx / max(total, 1)) * 55)
                self._emit(pct, f'Table {idx+1}/{total}: {table}')

                if not no_schema:
                    cur.execute("""
                        SELECT column_name,data_type,data_length,
                               data_precision,data_scale,nullable,data_default
                        FROM user_tab_columns WHERE table_name=:t ORDER BY column_id
                    """, t=table)
                    cols = cur.fetchall()
                    f.write(f'\n-- Table: {table}\nDROP TABLE "{table}" CASCADE CONSTRAINTS;\n')
                    f.write(f'CREATE TABLE "{table}" (\n')
                    defs = []
                    for name,dtype,dlen,dprec,dscale,nullable,default in cols:
                        d = f'    "{name}" {dtype}'
                        if dtype in ('VARCHAR2','CHAR','NVARCHAR2','NCHAR') and dlen:
                            d += f'({dlen})'
                        elif dtype == 'NUMBER' and dprec:
                            d += f'({dprec},{dscale or 0})'
                        if default:
                            d += f' DEFAULT {default.strip()}'
                        if nullable == 'N':
                            d += ' NOT NULL'
                        defs.append(d)
                    f.write(',\n'.join(defs) + '\n);\n')

                if not no_data:
                    cur.execute(f'SELECT * FROM "{table}"')
                    col_names = [d[0] for d in cur.description]
                    cols_str = ', '.join(f'"{c}"' for c in col_names)
                    chunk = cur.fetchmany(200)
                    while chunk:
                        for row in chunk:
                            vals = [_safe_val(v) for v in row]
                            f.write(f'INSERT INTO "{table}" ({cols_str}) VALUES ({", ".join(vals)});\n')
                        chunk = cur.fetchmany(200)

            if not no_schema:
                self._emit(76, 'Primary keys…')
                cur.execute("""
                    SELECT c.constraint_name, c.table_name,
                           listagg(cc.column_name,',') WITHIN GROUP (ORDER BY cc.position)
                    FROM user_constraints c
                    JOIN user_cons_columns cc ON cc.constraint_name=c.constraint_name
                    WHERE c.constraint_type='P'
                    GROUP BY c.constraint_name, c.table_name
                """)
                f.write('\n-- Primary Keys\n')
                for cname,tname,ca in cur.fetchall():
                    cols_q = ', '.join(f'"{c}"' for c in ca.split(','))
                    f.write(f'ALTER TABLE "{tname}" ADD CONSTRAINT "{cname}" PRIMARY KEY ({cols_q});\n')

                self._emit(79, 'Foreign keys…')
                cur.execute("""
                    SELECT c.constraint_name, c.table_name,
                           listagg(cc.column_name,',') WITHIN GROUP (ORDER BY cc.position),
                           c.r_constraint_name, c.delete_rule
                    FROM user_constraints c
                    JOIN user_cons_columns cc ON cc.constraint_name=c.constraint_name
                    WHERE c.constraint_type='R'
                    GROUP BY c.constraint_name,c.table_name,c.r_constraint_name,c.delete_rule
                """)
                f.write('\n-- Foreign Keys\n')
                for cname,tname,ca,rcname,dr in cur.fetchall():
                    cols_q = ', '.join(f'"{c}"' for c in ca.split(','))
                    cur2 = conn.cursor()
                    cur2.execute("""
                        SELECT table_name,
                               listagg(column_name,',') WITHIN GROUP (ORDER BY position)
                        FROM user_cons_columns WHERE constraint_name=:c GROUP BY table_name
                    """, c=rcname)
                    rrow = cur2.fetchone()
                    if rrow:
                        rtn, rca = rrow
                        rcols_q = ', '.join(f'"{c}"' for c in rca.split(','))
                        f.write(
                            f'ALTER TABLE "{tname}" ADD CONSTRAINT "{cname}" '
                            f'FOREIGN KEY ({cols_q}) REFERENCES "{rtn}" ({rcols_q}) ON DELETE {dr};\n'
                        )

                self._emit(82, 'Indexes…')
                cur.execute("""
                    SELECT index_name,table_name,uniqueness,
                           listagg(column_name,',') WITHIN GROUP (ORDER BY column_position)
                    FROM user_ind_columns JOIN user_indexes USING (index_name,table_name)
                    WHERE index_type='NORMAL'
                      AND index_name NOT IN (
                          SELECT constraint_name FROM user_constraints
                          WHERE constraint_type IN ('P','U'))
                    GROUP BY index_name,table_name,uniqueness ORDER BY table_name,index_name
                """)
                f.write('\n-- Indexes\n')
                for iname,tname,uniq,ca in cur.fetchall():
                    cols_q = ', '.join(f'"{c}"' for c in ca.split(','))
                    f.write(f'CREATE {"UNIQUE " if uniq=="UNIQUE" else ""}INDEX "{iname}" ON "{tname}" ({cols_q});\n')

                self._emit(86, 'Views…')
                cur.execute("SELECT view_name,text FROM user_views ORDER BY view_name")
                f.write('\n-- Views\n')
                for vname, vtext in cur.fetchall():
                    f.write(f'CREATE OR REPLACE VIEW "{vname}" AS\n{vtext};\n\n')

                self._emit(90, 'Triggers…')
                cur.execute("""
                    SELECT trigger_name,trigger_type,triggering_event,table_name,trigger_body
                    FROM user_triggers ORDER BY trigger_name
                """)
                f.write('\n-- Triggers\n')
                for tname,ttype,tevent,tbl,tbody in cur.fetchall():
                    f.write(
                        f'CREATE OR REPLACE TRIGGER "{tname}"\n'
                        f'{ttype} {tevent} ON "{tbl}"\n{tbody};\n/\n\n'
                    )

                self._emit(93, 'Stored procedures, functions & packages…')
                cur.execute("""
                    SELECT name,type,text FROM user_source
                    WHERE type IN ('PROCEDURE','FUNCTION','PACKAGE','PACKAGE BODY')
                    ORDER BY type,name,line
                """)
                f.write('\n-- Stored code\n')
                current = None
                for name,stype,line_text in cur.fetchall():
                    key = (stype, name)
                    if key != current:
                        if current is not None:
                            f.write('/\n\n')
                        f.write('CREATE OR REPLACE ')
                        current = key
                    f.write(line_text)
                if current:
                    f.write('/\n\n')

        cur.close(); conn.close()
        self._emit(98, 'Finalizing…')
        return True
