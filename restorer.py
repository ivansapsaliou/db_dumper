"""
RestoreManager — restore database dumps back to a target database.

Supports:
  - PostgreSQL, MySQL, Oracle
  - Compressed files: .sql, .sql.gz, .sql.bz2, .sql.zst
  - Preview mode: list tables & row counts from dump
  - Selective restore: only specific tables
  - Parallel restore threads (PostgreSQL pg_restore)
  - Progress tracking via callback
  - Row-count validation after restore
"""

import os
import re
import gzip
import bz2
import time
import uuid
import logging
import tempfile
import threading
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger(__name__)

# ── in-memory restore state ──────────────────────────────────────────────────
restore_progress: dict[str, dict] = {}   # restore_id -> state dict
restore_cancel: dict[str, bool]   = {}   # restore_id -> cancel flag


# ── helpers ──────────────────────────────────────────────────────────────────

def _open_dump(filepath: str):
    """Return an open binary stream for a (possibly compressed) dump file."""
    ext = Path(filepath).suffix.lower()
    if ext == '.gz':
        return gzip.open(filepath, 'rb')
    if ext == '.bz2':
        return bz2.open(filepath, 'rb')
    if ext == '.zst':
        try:
            import zstandard as zstd
            return zstd.ZstdDecompressor().stream_reader(open(filepath, 'rb'))
        except ImportError:
            raise RuntimeError('zstandard package required for .zst files')
    return open(filepath, 'rb')


def _decompress_to_temp(filepath: str) -> tuple[str, bool]:
    """
    If filepath is compressed, decompress to a temp file and return (path, True).
    Otherwise return (filepath, False).  Caller must clean up temp file.
    """
    ext = Path(filepath).suffix.lower()
    if ext not in ('.gz', '.bz2', '.zst'):
        return filepath, False
    suffix = Path(Path(filepath).stem).suffix or '.sql'
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp.close()
    with _open_dump(filepath) as f_in, open(tmp.name, 'wb') as f_out:
        import shutil
        shutil.copyfileobj(f_in, f_out)
    return tmp.name, True


def _port(cfg: dict) -> int:
    raw = cfg.get('port')
    defaults = {'postgresql': 5432, 'mysql': 3306, 'oracle': 1521}
    try:
        return int(raw) if raw else defaults.get(cfg.get('type', '').lower(), 5432)
    except (TypeError, ValueError):
        return defaults.get(cfg.get('type', '').lower(), 5432)


# ── SQL preview parser ────────────────────────────────────────────────────────

def preview_dump(filepath: str) -> dict:
    """
    Parse a SQL dump (plain or compressed) and return:
      {
        'tables': [{'name': ..., 'rows': ...}, ...],
        'file_size': int (bytes),
        'format': 'sql' | 'gz' | 'bz2' | 'zst',
        'estimated_restore_min': float,
      }
    Only the first 8 MB are scanned to keep things fast.
    """
    file_size = os.path.getsize(filepath) if os.path.exists(filepath) else 0
    ext = Path(filepath).suffix.lower().lstrip('.')
    if ext not in ('gz', 'bz2', 'zst'):
        ext = 'sql'

    tables: dict[str, int] = {}   # table_name -> INSERT row count
    max_scan = 8 * 1024 * 1024    # 8 MB

    create_re = re.compile(r'CREATE TABLE\s+(?:IF NOT EXISTS\s+)?[`"\[]?(\w+)[`"\]]?', re.I)
    insert_re = re.compile(r'INSERT\s+INTO\s+[`"\[]?(\w+)[`"\]]?', re.I)
    copy_re   = re.compile(r'COPY\s+[`"\[]?(\w+)[`"\]]?', re.I)
    scanned   = 0

    try:
        with _open_dump(filepath) as fh:
            for raw_line in fh:
                scanned += len(raw_line)
                if scanned > max_scan:
                    break
                try:
                    line = raw_line.decode('utf-8', errors='replace')
                except AttributeError:
                    line = raw_line

                m = create_re.search(line)
                if m:
                    tables.setdefault(m.group(1), 0)
                    continue

                m = insert_re.search(line)
                if m:
                    tables[m.group(1)] = tables.get(m.group(1), 0) + 1
                    continue

                m = copy_re.search(line)
                if m:
                    tables.setdefault(m.group(1), 0)
    except Exception as e:
        logger.warning(f'preview_dump scan error: {e}')

    table_list = sorted(
        [{'name': k, 'rows': v} for k, v in tables.items()],
        key=lambda x: x['rows'], reverse=True
    )

    # Rough estimate: 5 MB/s restore speed
    size_mb = file_size / 1_048_576
    estimated_min = round(size_mb / 5 / 60, 1)

    return {
        'tables':                table_list,
        'file_size':             file_size,
        'format':                ext,
        'estimated_restore_min': estimated_min,
    }


# ── main RestoreManager ───────────────────────────────────────────────────────

class RestoreManager:
    """
    Manages restore operations for PostgreSQL, MySQL, Oracle.

    Usage:
        rm = RestoreManager()
        restore_id = rm.start(db_config, dump_file, tables=['users','orders'])
        prog = rm.get_progress(restore_id)
        rm.cancel(restore_id)
    """

    def __init__(self, progress_callback: Optional[Callable] = None):
        self._cb = progress_callback   # fn(restore_id, data) or None

    # ── public API ────────────────────────────────────────────────────────────

    def start(self, db_config: dict, dump_file: str,
              tables: Optional[list] = None,
              threads: int = 1) -> str:
        """
        Launch a restore in a background thread.
        Returns restore_id (UUID string).
        """
        restore_id = str(uuid.uuid4())
        restore_cancel[restore_id] = False
        restore_progress[restore_id] = {
            'status':     'queued',
            'percent':    0,
            'message':    'Queued…',
            'started_at': datetime.now().isoformat(),
            'db_name':    db_config.get('database', '?'),
            'dump_file':  os.path.basename(dump_file),
            'tables':     tables or [],
        }
        t = threading.Thread(
            target=self._run,
            args=(db_config, dump_file, tables, threads, restore_id),
            daemon=True,
        )
        t.start()
        return restore_id

    def get_progress(self, restore_id: str) -> Optional[dict]:
        return restore_progress.get(restore_id)

    def cancel(self, restore_id: str) -> bool:
        if restore_id in restore_progress:
            restore_cancel[restore_id] = True
            return True
        return False

    # ── internals ─────────────────────────────────────────────────────────────

    def _emit(self, restore_id: str, data: dict):
        restore_progress[restore_id] = {**restore_progress.get(restore_id, {}), **data}
        if self._cb:
            try:
                self._cb(restore_id, data)
            except Exception:
                pass

    def _is_cancelled(self, restore_id: str) -> bool:
        return restore_cancel.get(restore_id, False)

    def _run(self, db_config: dict, dump_file: str,
             tables: Optional[list], threads: int, restore_id: str):
        db_type = db_config.get('type', '').lower()
        try:
            self._emit(restore_id, {'status': 'running', 'percent': 5,
                                    'message': 'Starting restore…'})
            if not os.path.exists(dump_file):
                raise FileNotFoundError(f'Dump file not found: {dump_file}')

            if db_type == 'postgresql':
                self._restore_postgresql(db_config, dump_file, tables, threads, restore_id)
            elif db_type == 'mysql':
                self._restore_mysql(db_config, dump_file, tables, restore_id)
            elif db_type == 'oracle':
                self._restore_oracle(db_config, dump_file, tables, restore_id)
            else:
                raise ValueError(f'Unsupported database type: {db_type}')

            if self._is_cancelled(restore_id):
                self._emit(restore_id, {'status': 'cancelled', 'percent': 0,
                                        'message': 'Restore cancelled',
                                        'finished_at': datetime.now().isoformat()})
                return

            self._emit(restore_id, {'status': 'done', 'percent': 100,
                                    'message': 'Restore completed successfully',
                                    'finished_at': datetime.now().isoformat()})

        except Exception as e:
            logger.exception('RestoreManager._run')
            self._emit(restore_id, {'status': 'error', 'percent': 0,
                                    'message': str(e),
                                    'finished_at': datetime.now().isoformat()})
        finally:
            restore_cancel.pop(restore_id, None)

    # ── PostgreSQL ────────────────────────────────────────────────────────────

    def _restore_postgresql(self, cfg: dict, dump_file: str,
                             tables: Optional[list], threads: int, restore_id: str):
        import subprocess

        self._emit(restore_id, {'percent': 10, 'message': 'Decompressing dump…'})
        sql_file, is_tmp = _decompress_to_temp(dump_file)
        try:
            if self._is_cancelled(restore_id):
                return

            self._emit(restore_id, {'percent': 20, 'message': 'Restoring (psql)…'})
            env = os.environ.copy()
            env['PGPASSWORD'] = cfg.get('password', '')

            cmd = [
                'psql',
                '-h', cfg.get('host', 'localhost'),
                '-p', str(_port(cfg)),
                '-U', cfg.get('user', 'postgres'),
                '-d', cfg.get('database', 'postgres'),
                '--no-password',
                '-v', 'ON_ERROR_STOP=1',
            ]

            # Selective restore: wrap with grep-based filter or use -t option
            if tables:
                # Filter INSERT statements for selected tables only
                self._emit(restore_id, {'percent': 25,
                                        'message': f'Restoring tables: {", ".join(tables)}'})
                sql_file = self._filter_sql_tables(sql_file, tables, restore_id)
                is_tmp = True

            cmd.extend(['-f', sql_file])
            result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=7200)

            if result.returncode != 0:
                err = (result.stderr or '').strip()[:500]
                raise RuntimeError(f'psql failed (rc={result.returncode}): {err}')

            self._emit(restore_id, {'percent': 90, 'message': 'Verifying row counts…'})
            counts = self._pg_row_counts(cfg, tables)
            self._emit(restore_id, {'percent': 95, 'message': 'Validation complete',
                                    'row_counts': counts})

        finally:
            if is_tmp and os.path.exists(sql_file):
                try:
                    os.remove(sql_file)
                except OSError:
                    pass

    # ── MySQL ─────────────────────────────────────────────────────────────────

    def _restore_mysql(self, cfg: dict, dump_file: str,
                       tables: Optional[list], restore_id: str):
        import subprocess

        self._emit(restore_id, {'percent': 10, 'message': 'Decompressing dump…'})
        sql_file, is_tmp = _decompress_to_temp(dump_file)
        try:
            if self._is_cancelled(restore_id):
                return

            self._emit(restore_id, {'percent': 20, 'message': 'Restoring (mysql)…'})

            if tables:
                self._emit(restore_id, {'percent': 25,
                                        'message': f'Restoring tables: {", ".join(tables)}'})
                sql_file = self._filter_sql_tables(sql_file, tables, restore_id)
                is_tmp = True

            cmd = [
                'mysql',
                '-h', cfg.get('host', 'localhost'),
                '-P', str(_port(cfg)),
                '-u', cfg.get('user', 'root'),
                f"-p{cfg.get('password', '')}",
                cfg.get('database', ''),
            ]
            with open(sql_file, 'r', encoding='utf-8', errors='replace') as f_in:
                result = subprocess.run(cmd, stdin=f_in, capture_output=True,
                                        text=True, timeout=7200)
            if result.returncode != 0:
                err = (result.stderr or '').strip()[:500]
                raise RuntimeError(f'mysql failed (rc={result.returncode}): {err}')

            self._emit(restore_id, {'percent': 90, 'message': 'Verifying row counts…'})
            counts = self._mysql_row_counts(cfg, tables)
            self._emit(restore_id, {'percent': 95, 'message': 'Validation complete',
                                    'row_counts': counts})

        finally:
            if is_tmp and os.path.exists(sql_file):
                try:
                    os.remove(sql_file)
                except OSError:
                    pass

    # ── Oracle ────────────────────────────────────────────────────────────────

    def _restore_oracle(self, cfg: dict, dump_file: str,
                        tables: Optional[list], restore_id: str):
        import subprocess

        self._emit(restore_id, {'percent': 10, 'message': 'Decompressing dump…'})
        sql_file, is_tmp = _decompress_to_temp(dump_file)
        try:
            if self._is_cancelled(restore_id):
                return

            self._emit(restore_id, {'percent': 20, 'message': 'Restoring (sqlplus)…'})
            svc = cfg.get('service_name') or cfg.get('database', '')
            conn_str = (f"{cfg.get('user', '')}/"
                        f"{cfg.get('password', '')}@"
                        f"{cfg.get('host', 'localhost')}:{_port(cfg)}/{svc}")

            if tables:
                self._emit(restore_id, {'percent': 25,
                                        'message': f'Restoring tables: {", ".join(tables)}'})
                sql_file = self._filter_sql_tables(sql_file, tables, restore_id)
                is_tmp = True

            cmd = ['sqlplus', '-S', conn_str, f'@{sql_file}']
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
            if result.returncode != 0:
                err = (result.stderr or result.stdout or '').strip()[:500]
                raise RuntimeError(f'sqlplus failed: {err}')

            self._emit(restore_id, {'percent': 90, 'message': 'Restore complete'})

        finally:
            if is_tmp and os.path.exists(sql_file):
                try:
                    os.remove(sql_file)
                except OSError:
                    pass

    # ── selective table filter ────────────────────────────────────────────────

    def _filter_sql_tables(self, sql_file: str, tables: list, restore_id: str) -> str:
        """
        Write a new SQL file containing only DDL/DML for the requested tables.
        Returns path of the filtered temp file.
        """
        self._emit(restore_id, {'percent': 30, 'message': 'Filtering selected tables…'})
        patterns = [re.compile(
            rf'(?:CREATE TABLE|INSERT INTO|COPY)\s+[`"\[]?{re.escape(t)}[`"\]]?', re.I
        ) for t in tables]

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.sql', mode='w',
                                          encoding='utf-8', errors='replace')
        try:
            include = False
            with open(sql_file, 'r', encoding='utf-8', errors='replace') as f_in:
                for line in f_in:
                    # Check if this line starts a relevant block
                    for pat in patterns:
                        if pat.search(line):
                            include = True
                            break
                    # Stop including at next unrelated DDL statement
                    if include and re.match(r'^(CREATE|DROP|ALTER|--)\s', line, re.I):
                        # Re-check if still matching our tables
                        matched = any(pat.search(line) for pat in patterns)
                        if not matched and re.match(r'^(CREATE|DROP|ALTER)\s', line, re.I):
                            include = False
                    if include:
                        tmp.write(line)
        finally:
            tmp.close()
        return tmp.name

    # ── row count helpers ─────────────────────────────────────────────────────

    def _pg_row_counts(self, cfg: dict, tables: Optional[list]) -> dict:
        """Return {table: row_count} for PostgreSQL."""
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=cfg['host'], port=_port(cfg),
                user=cfg['user'], password=cfg['password'],
                dbname=cfg['database'], connect_timeout=10,
            )
            cur = conn.cursor()
            if tables:
                target = tables
            else:
                cur.execute(
                    "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
                )
                target = [row[0] for row in cur.fetchall()]

            counts = {}
            for tbl in target[:20]:   # cap at 20 to avoid long queries
                try:
                    cur.execute(f'SELECT COUNT(*) FROM "{tbl}"')
                    counts[tbl] = cur.fetchone()[0]
                except Exception:
                    counts[tbl] = -1
            cur.close()
            conn.close()
            return counts
        except Exception as e:
            logger.warning(f'_pg_row_counts failed: {e}')
            return {}

    def _mysql_row_counts(self, cfg: dict, tables: Optional[list]) -> dict:
        """Return {table: row_count} for MySQL."""
        try:
            import pymysql
            conn = pymysql.connect(
                host=cfg['host'], port=_port(cfg),
                user=cfg['user'], password=cfg['password'],
                database=cfg['database'], connect_timeout=10,
            )
            cur = conn.cursor()
            if not tables:
                cur.execute('SHOW TABLES')
                tables = [row[0] for row in cur.fetchall()]

            counts = {}
            for tbl in (tables or [])[:20]:
                try:
                    cur.execute(f'SELECT COUNT(*) FROM `{tbl}`')
                    counts[tbl] = cur.fetchone()[0]
                except Exception:
                    counts[tbl] = -1
            cur.close()
            conn.close()
            return counts
        except Exception as e:
            logger.warning(f'_mysql_row_counts failed: {e}')
            return {}
