"""
DatabaseDumper — performs SQL dumps for PostgreSQL, MySQL, Oracle
via SSH tunnel (paramiko) or direct connection — without requiring
any native client binaries on the local machine.
"""

import io
import os
import time
import logging
import threading
from datetime import datetime

import paramiko

logger = logging.getLogger(__name__)


# ── helpers ───────────────────────────────────────────────────────────────────

def _fmt_size(n: int) -> str:
    for unit in ('B', 'KB', 'MB', 'GB'):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


class DatabaseDumper:
    def __init__(self, db_config: dict, socketio, dump_id, progress_cb):
        self.cfg = db_config
        self.socketio = socketio
        self.dump_id = dump_id
        self.progress_cb = progress_cb

    # ── progress helper ───────────────────────────────────────────────────────

    def _emit(self, percent: int, message: str, status: str = 'running'):
        if self.progress_cb:
            self.progress_cb(self.dump_id, {
                'status': status,
                'percent': percent,
                'message': message
            })

    # ── SSH tunnel ────────────────────────────────────────────────────────────

    def _ssh_client(self):
        cfg = self.cfg
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        connect_kwargs = dict(
            hostname=cfg['ssh_host'],
            port=int(cfg.get('ssh_port', 22)),
            username=cfg['ssh_user'],
            timeout=30,
        )
        if cfg.get('ssh_key'):
            key_path = os.path.expanduser(cfg['ssh_key'])
            connect_kwargs['key_filename'] = key_path
        else:
            connect_kwargs['password'] = cfg.get('ssh_password', '')

        client.connect(**connect_kwargs)
        return client

    # ── connection test ───────────────────────────────────────────────────────

    def test_connection(self):
        try:
            db_type = self.cfg.get('type', '').lower()
            use_ssh = self.cfg.get('use_ssh', False)

            if use_ssh:
                client = self._ssh_client()
                # quick check: list remote directory
                _, stdout, stderr = client.exec_command('echo OK')
                out = stdout.read().decode().strip()
                client.close()
                if out != 'OK':
                    return False, 'SSH connection failed'

                # now try DB via remote python snippet
                ok, msg = self._test_db_via_ssh(client if False else None, db_type)
            else:
                ok, msg = self._test_db_direct(db_type)

            return ok, msg
        except Exception as e:
            return False, str(e)

    def _test_db_direct(self, db_type):
        cfg = self.cfg
        try:
            if db_type == 'postgresql':
                import psycopg2
                conn = psycopg2.connect(
                    host=cfg['host'], port=int(cfg.get('port', 5432)),
                    user=cfg['user'], password=cfg['password'],
                    dbname=cfg['database'], connect_timeout=10
                )
                conn.close()
            elif db_type == 'mysql':
                import pymysql
                conn = pymysql.connect(
                    host=cfg['host'], port=int(cfg.get('port', 3306)),
                    user=cfg['user'], password=cfg['password'],
                    database=cfg['database'], connect_timeout=10
                )
                conn.close()
            elif db_type == 'oracle':
                import cx_Oracle
                dsn = cx_Oracle.makedsn(
                    cfg['host'], int(cfg.get('port', 1521)),
                    service_name=cfg.get('service_name') or cfg.get('database')
                )
                conn = cx_Oracle.connect(cfg['user'], cfg['password'], dsn)
                conn.close()
            else:
                return False, f'Unknown DB type: {db_type}'
            return True, 'Connection successful'
        except Exception as e:
            return False, str(e)

    def _test_db_via_ssh(self, _unused, db_type):
        """Run a minimal python snippet on the remote host to verify DB connectivity."""
        cfg = self.cfg
        client = self._ssh_client()
        try:
            if db_type == 'postgresql':
                script = (
                    f"python3 -c \""
                    f"import psycopg2; "
                    f"c=psycopg2.connect(host='{cfg['host']}',port={cfg.get('port',5432)},"
                    f"user='{cfg['user']}',password='{cfg['password']}',dbname='{cfg['database']}'); "
                    f"c.close(); print('OK')\""
                )
            elif db_type == 'mysql':
                script = (
                    f"python3 -c \""
                    f"import pymysql; "
                    f"c=pymysql.connect(host='{cfg['host']}',port={cfg.get('port',3306)},"
                    f"user='{cfg['user']}',password='{cfg['password']}',database='{cfg['database']}'); "
                    f"c.close(); print('OK')\""
                )
            elif db_type == 'oracle':
                script = (
                    f"python3 -c \""
                    f"import cx_Oracle; "
                    f"d=cx_Oracle.makedsn('{cfg['host']}',{cfg.get('port',1521)},service_name='{cfg.get('service_name',cfg['database'])}'); "
                    f"c=cx_Oracle.connect('{cfg['user']}','{cfg['password']}',d); "
                    f"c.close(); print('OK')\""
                )
            else:
                return False, f'Unknown DB type: {db_type}'

            _, stdout, stderr = client.exec_command(script)
            out = stdout.read().decode().strip()
            err = stderr.read().decode().strip()
            if out == 'OK':
                return True, 'Connection via SSH successful'
            return False, err or 'Remote test failed'
        finally:
            client.close()

    # ── main dump entry point ─────────────────────────────────────────────────

    def dump(self, filepath: str) -> bool:
        db_type = self.cfg.get('type', '').lower()
        use_ssh = self.cfg.get('use_ssh', False)

        self._emit(5, f'Connecting ({db_type.upper()})...')

        if use_ssh:
            return self._dump_via_ssh(db_type, filepath)
        else:
            return self._dump_direct(db_type, filepath)

    # ── direct dump (no SSH) ──────────────────────────────────────────────────

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
            logger.exception(e)
            return False

    def _pg_dump_direct(self, filepath: str) -> bool:
        import psycopg2
        cfg = self.cfg
        conn = psycopg2.connect(
            host=cfg['host'], port=int(cfg.get('port', 5432)),
            user=cfg['user'], password=cfg['password'],
            dbname=cfg['database']
        )
        self._emit(10, 'Connected. Fetching schema...')
        include_tables = cfg.get('include_tables', [])
        exclude_tables = cfg.get('exclude_tables', [])
        no_data = cfg.get('no_data', False)
        no_schema = cfg.get('no_schema', False)

        with conn.cursor() as cur:
            # Get all tables
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            all_tables = [r[0] for r in cur.fetchall()]

        if include_tables:
            tables = [t for t in all_tables if t in include_tables]
        else:
            tables = [t for t in all_tables if t not in exclude_tables]

        total = len(tables)
        self._emit(15, f'Found {total} tables. Dumping...')

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"-- PostgreSQL dump: {cfg['database']}\n")
            f.write(f"-- Generated: {datetime.now().isoformat()}\n")
            f.write("SET client_encoding = 'UTF8';\n")
            f.write("SET standard_conforming_strings = on;\n\n")

            with conn.cursor() as cur:
                for idx, table in enumerate(tables):
                    pct = 15 + int((idx / max(total, 1)) * 80)
                    self._emit(pct, f'Dumping table: {table} ({idx+1}/{total})')

                    if not no_schema:
                        # DDL via pg_dump logic using information_schema
                        f.write(f"\n-- Table: {table}\n")
                        f.write(f"DROP TABLE IF EXISTS \"{table}\" CASCADE;\n")
                        cur.execute(f"""
                            SELECT column_name, data_type, character_maximum_length,
                                   is_nullable, column_default
                            FROM information_schema.columns
                            WHERE table_schema='public' AND table_name=%s
                            ORDER BY ordinal_position
                        """, (table,))
                        cols = cur.fetchall()
                        col_defs = []
                        for col in cols:
                            name, dtype, maxlen, nullable, default = col
                            col_def = f'    "{name}" {dtype}'
                            if maxlen:
                                col_def += f'({maxlen})'
                            if default is not None:
                                col_def += f' DEFAULT {default}'
                            if nullable == 'NO':
                                col_def += ' NOT NULL'
                            col_defs.append(col_def)
                        f.write(f"CREATE TABLE \"{table}\" (\n")
                        f.write(',\n'.join(col_defs))
                        f.write('\n);\n')

                    if not no_data:
                        # Data
                        cur.execute(f'SELECT * FROM "{table}"')
                        rows = cur.fetchall()
                        col_names = [desc[0] for desc in cur.description]
                        if rows:
                            cols_str = ', '.join(f'"{c}"' for c in col_names)
                            f.write(f"\n-- Data for {table}\n")
                            chunk_size = 500
                            for i in range(0, len(rows), chunk_size):
                                chunk = rows[i:i+chunk_size]
                                vals_list = []
                                for row in chunk:
                                    vals = []
                                    for v in row:
                                        if v is None:
                                            vals.append('NULL')
                                        elif isinstance(v, bool):
                                            vals.append('TRUE' if v else 'FALSE')
                                        elif isinstance(v, (int, float)):
                                            vals.append(str(v))
                                        else:
                                            escaped = str(v).replace("'", "''")
                                            vals.append(f"'{escaped}'")
                                    vals_list.append('(' + ', '.join(vals) + ')')
                                f.write(f"INSERT INTO \"{table}\" ({cols_str}) VALUES\n")
                                f.write(',\n'.join(vals_list) + ';\n')

        conn.close()
        self._emit(98, 'Finalizing...')
        return True

    def _mysql_dump_direct(self, filepath: str) -> bool:
        import pymysql
        cfg = self.cfg
        conn = pymysql.connect(
            host=cfg['host'], port=int(cfg.get('port', 3306)),
            user=cfg['user'], password=cfg['password'],
            database=cfg['database'], charset='utf8mb4'
        )
        self._emit(10, 'Connected. Fetching schema...')
        include_tables = cfg.get('include_tables', [])
        exclude_tables = cfg.get('exclude_tables', [])
        no_data = cfg.get('no_data', False)
        no_schema = cfg.get('no_schema', False)

        with conn.cursor() as cur:
            cur.execute("SHOW TABLES")
            all_tables = [r[0] for r in cur.fetchall()]

        if include_tables:
            tables = [t for t in all_tables if t in include_tables]
        else:
            tables = [t for t in all_tables if t not in exclude_tables]

        total = len(tables)
        self._emit(15, f'Found {total} tables. Dumping...')

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"-- MySQL dump: {cfg['database']}\n")
            f.write(f"-- Generated: {datetime.now().isoformat()}\n")
            f.write("SET NAMES utf8mb4;\n")
            f.write("SET FOREIGN_KEY_CHECKS=0;\n\n")

            with conn.cursor() as cur:
                for idx, table in enumerate(tables):
                    pct = 15 + int((idx / max(total, 1)) * 80)
                    self._emit(pct, f'Dumping table: {table} ({idx+1}/{total})')

                    if not no_schema:
                        cur.execute(f"SHOW CREATE TABLE `{table}`")
                        row = cur.fetchone()
                        f.write(f"\n-- Table: {table}\n")
                        f.write(f"DROP TABLE IF EXISTS `{table}`;\n")
                        f.write(row[1] + ';\n')

                    if not no_data:
                        cur.execute(f"SELECT * FROM `{table}`")
                        rows = cur.fetchall()
                        col_names = [desc[0] for desc in cur.description]
                        if rows:
                            cols_str = ', '.join(f'`{c}`' for c in col_names)
                            f.write(f"\n-- Data for {table}\n")
                            chunk_size = 500
                            for i in range(0, len(rows), chunk_size):
                                chunk = rows[i:i+chunk_size]
                                vals_list = []
                                for row in chunk:
                                    vals = []
                                    for v in row:
                                        if v is None:
                                            vals.append('NULL')
                                        elif isinstance(v, (int, float)):
                                            vals.append(str(v))
                                        else:
                                            escaped = str(v).replace("'", "\\'")
                                            vals.append(f"'{escaped}'")
                                    vals_list.append('(' + ', '.join(vals) + ')')
                                f.write(f"INSERT INTO `{table}` ({cols_str}) VALUES\n")
                                f.write(',\n'.join(vals_list) + ';\n')

            f.write("\nSET FOREIGN_KEY_CHECKS=1;\n")

        conn.close()
        self._emit(98, 'Finalizing...')
        return True

    def _oracle_dump_direct(self, filepath: str) -> bool:
        import cx_Oracle
        cfg = self.cfg
        dsn = cx_Oracle.makedsn(
            cfg['host'], int(cfg.get('port', 1521)),
            service_name=cfg.get('service_name') or cfg.get('database')
        )
        conn = cx_Oracle.connect(cfg['user'], cfg['password'], dsn)
        self._emit(10, 'Connected. Fetching schema...')
        include_tables = cfg.get('include_tables', [])
        exclude_tables = cfg.get('exclude_tables', [])
        no_data = cfg.get('no_data', False)
        no_schema = cfg.get('no_schema', False)

        with conn.cursor() as cur:
            cur.execute("SELECT table_name FROM user_tables ORDER BY table_name")
            all_tables = [r[0] for r in cur.fetchall()]

        if include_tables:
            tables = [t for t in all_tables if t in include_tables]
        else:
            tables = [t for t in all_tables if t not in exclude_tables]

        total = len(tables)
        self._emit(15, f'Found {total} tables. Dumping...')

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"-- Oracle dump: {cfg['database']}\n")
            f.write(f"-- Generated: {datetime.now().isoformat()}\n\n")

            with conn.cursor() as cur:
                for idx, table in enumerate(tables):
                    pct = 15 + int((idx / max(total, 1)) * 80)
                    self._emit(pct, f'Dumping table: {table} ({idx+1}/{total})')

                    if not no_schema:
                        cur.execute(f"""
                            SELECT column_name, data_type, data_length, nullable, data_default
                            FROM user_tab_columns WHERE table_name=:t ORDER BY column_id
                        """, t=table)
                        cols = cur.fetchall()
                        f.write(f"\n-- Table: {table}\n")
                        f.write(f"DROP TABLE \"{table}\" CASCADE CONSTRAINTS;\n")
                        f.write(f"CREATE TABLE \"{table}\" (\n")
                        col_defs = []
                        for col in cols:
                            name, dtype, length, nullable, default = col
                            col_def = f'    "{name}" {dtype}'
                            if dtype in ('VARCHAR2', 'CHAR', 'NVARCHAR2') and length:
                                col_def += f'({length})'
                            if default:
                                col_def += f' DEFAULT {default.strip()}'
                            if nullable == 'N':
                                col_def += ' NOT NULL'
                            col_defs.append(col_def)
                        f.write(',\n'.join(col_defs))
                        f.write('\n);\n')

                    if not no_data:
                        cur.execute(f'SELECT * FROM "{table}"')
                        rows = cur.fetchall()
                        col_names = [desc[0] for desc in cur.description]
                        if rows:
                            cols_str = ', '.join(f'"{c}"' for c in col_names)
                            f.write(f"\n-- Data for {table}\n")
                            chunk_size = 200
                            for i in range(0, len(rows), chunk_size):
                                for row in rows[i:i+chunk_size]:
                                    vals = []
                                    for v in row:
                                        if v is None:
                                            vals.append('NULL')
                                        elif isinstance(v, (int, float)):
                                            vals.append(str(v))
                                        else:
                                            escaped = str(v).replace("'", "''")
                                            vals.append(f"'{escaped}'")
                                    f.write(f"INSERT INTO \"{table}\" ({cols_str}) VALUES ({', '.join(vals)});\n")

        conn.close()
        self._emit(98, 'Finalizing...')
        return True

    # ── SSH dump (run dump command on remote, stream back) ────────────────────

    def _dump_via_ssh(self, db_type: str, filepath: str) -> bool:
        cfg = self.cfg
        client = self._ssh_client()
        self._emit(10, 'SSH connected. Starting remote dump...')

        try:
            if db_type == 'postgresql':
                cmd = self._pg_ssh_cmd()
            elif db_type == 'mysql':
                cmd = self._mysql_ssh_cmd()
            elif db_type == 'oracle':
                # Oracle: run a python snippet on remote server
                return self._oracle_ssh_dump(client, filepath)
            else:
                self._emit(0, f'Unsupported DB type: {db_type}', 'error')
                return False

            self._emit(20, f'Running: {db_type.upper()} dump on remote server...')
            _, stdout, stderr = client.exec_command(cmd)

            received = 0
            with open(filepath, 'wb') as f:
                while True:
                    chunk = stdout.read(65536)
                    if not chunk:
                        break
                    f.write(chunk)
                    received += len(chunk)
                    mb = received / (1024 * 1024)
                    self._emit(min(90, 20 + int(mb)), f'Received: {mb:.1f} MB...')

            err = stderr.read().decode()
            if err and 'error' in err.lower():
                self._emit(0, f'Remote error: {err}', 'error')
                return False

            self._emit(98, 'Finalizing...')
            return True
        finally:
            client.close()

    def _pg_ssh_cmd(self) -> str:
        cfg = self.cfg
        env = f"PGPASSWORD='{cfg['password']}'"
        cmd = (
            f"{env} pg_dump -h {cfg['host']} -p {cfg.get('port', 5432)} "
            f"-U {cfg['user']} {cfg['database']}"
        )
        if cfg.get('no_data'):
            cmd += ' --schema-only'
        if cfg.get('no_schema'):
            cmd += ' --data-only'
        for t in cfg.get('include_tables', []):
            cmd += f' -t {t}'
        for t in cfg.get('exclude_tables', []):
            cmd += f' -T {t}'
        return cmd

    def _mysql_ssh_cmd(self) -> str:
        cfg = self.cfg
        cmd = (
            f"mysqldump -h {cfg['host']} -P {cfg.get('port', 3306)} "
            f"-u {cfg['user']} -p'{cfg['password']}' {cfg['database']}"
        )
        if cfg.get('no_data'):
            cmd += ' --no-data'
        if cfg.get('no_schema'):
            cmd += ' --no-create-info'
        for t in cfg.get('include_tables', []):
            cmd += f' {t}'
        return cmd

    def _oracle_ssh_dump(self, client, filepath: str) -> bool:
        cfg = self.cfg
        # Build python script to run on remote
        script = f"""
import cx_Oracle, sys
dsn=cx_Oracle.makedsn('{cfg['host']}',{cfg.get('port',1521)},service_name='{cfg.get('service_name',cfg['database'])}')
conn=cx_Oracle.connect('{cfg['user']}','{cfg['password']}',dsn)
cur=conn.cursor()
cur.execute('SELECT table_name FROM user_tables ORDER BY table_name')
tables=[r[0] for r in cur.fetchall()]
print('-- Oracle dump')
for t in tables:
    cur.execute(f'SELECT * FROM "{{t}}"')
    rows=cur.fetchall()
    cols=[d[0] for d in cur.description]
    cols_s=','.join(f'"{{c}}"' for c in cols)
    for row in rows:
        vals=[]
        for v in row:
            if v is None: vals.append('NULL')
            elif isinstance(v,(int,float)): vals.append(str(v))
            else: vals.append("'"+str(v).replace("'","''")+"'")
        print(f'INSERT INTO "{{t}}" ({{cols_s}}) VALUES ({{",".join(vals)}});')
conn.close()
"""
        _, stdout, stderr = client.exec_command(f"python3 -c \"{script}\"")
        received = 0
        with open(filepath, 'wb') as f:
            while True:
                chunk = stdout.read(65536)
                if not chunk:
                    break
                f.write(chunk)
                received += len(chunk)
                self._emit(min(90, 20 + received // (1024*1024)), f'Received: {received//(1024*1024)} MB...')

        err = stderr.read().decode()
        if err and 'error' in err.lower():
            self._emit(0, f'Remote error: {err}', 'error')
            return False
        return True
