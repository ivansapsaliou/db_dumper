"""
DumpVerifier — post-download integrity checks.

For .sql files  : counts tables/inserts, checks header
For .dump files : runs pg_restore --list locally (if available) or checks magic bytes
For .gz files   : checks gzip integrity
For .tar files  : checks tar member list
"""

import os
import gzip
import tarfile
import logging
import subprocess
import re
from pathlib import Path

logger = logging.getLogger(__name__)


class DumpVerifier:

    def verify(self, filepath: str, db_type: str = 'postgresql') -> dict:
        """
        Returns:
          {
            ok: bool,
            checks: [{name, ok, detail}],
            summary: str,
            tables_found: int,
            inserts_found: int,
            file_size: int,
          }
        """
        if not os.path.exists(filepath):
            return {'ok': False, 'summary': 'File not found', 'checks': []}

        checks = []
        ext = Path(filepath).suffix.lower()

        # 1. File size
        size = os.path.getsize(filepath) if not os.path.isdir(filepath) else self._dir_size(filepath)
        checks.append({
            'name': 'File exists & non-empty',
            'ok':   size > 0,
            'detail': f'{self._fmt(size)}',
        })

        tables_found  = 0
        inserts_found = 0

        if os.path.isdir(filepath):
            c, t, i = self._verify_directory(filepath)
            checks += c; tables_found = t; inserts_found = i

        elif ext == '.gz':
            c, t, i = self._verify_gz(filepath)
            checks += c; tables_found = t; inserts_found = i

        elif ext == '.tar':
            c = self._verify_tar(filepath)
            checks += c

        elif ext in ('.dump', '.dmp'):
            c = self._verify_binary(filepath, db_type)
            checks += c

        elif ext == '.sql':
            c, t, i = self._verify_sql(filepath)
            checks += c; tables_found = t; inserts_found = i

        all_ok  = all(ch['ok'] for ch in checks)
        summary = f'{"✅ OK" if all_ok else "⚠️ Issues found"} — {len(checks)} checks'
        if tables_found:
            summary += f', {tables_found} tables'
        if inserts_found:
            summary += f', {inserts_found:,} INSERT blocks'

        return {
            'ok':            all_ok,
            'checks':        checks,
            'summary':       summary,
            'tables_found':  tables_found,
            'inserts_found': inserts_found,
            'file_size':     size,
        }

    # ── SQL plain ─────────────────────────────────────────────────────────────

    def _verify_sql(self, path: str):
        checks = []
        tables   = set()
        inserts  = 0
        has_header = False
        truncated  = False

        try:
            with open(path, 'r', encoding='utf-8', errors='replace') as f:
                for i, line in enumerate(f):
                    if i < 5 and ('dump' in line.lower() or 'generated' in line.lower()):
                        has_header = True
                    m = re.search(r'CREATE TABLE\s+"?(\w+)"?', line, re.I)
                    if m:
                        tables.add(m.group(1))
                    if line.strip().upper().startswith('INSERT INTO'):
                        inserts += 1

            checks.append({'name': 'Has dump header comment', 'ok': has_header,
                           'detail': 'Found' if has_header else 'Missing'})
            checks.append({'name': 'Contains CREATE TABLE statements',
                           'ok': len(tables) > 0,
                           'detail': f'{len(tables)} table definitions found'})
            checks.append({'name': 'Contains INSERT statements',
                           'ok': inserts > 0 or len(tables) == 0,
                           'detail': f'{inserts:,} INSERT blocks'})

        except Exception as e:
            checks.append({'name': 'SQL parse', 'ok': False, 'detail': str(e)})

        return checks, len(tables), inserts

    # ── GZip ──────────────────────────────────────────────────────────────────

    def _verify_gz(self, path: str):
        checks = []
        tables  = 0
        inserts = 0
        try:
            with gzip.open(path, 'rt', encoding='utf-8', errors='replace') as f:
                content = f.read(1024 * 1024)  # read first 1 MB to check
            checks.append({'name': 'GZip integrity', 'ok': True,
                           'detail': 'Archive readable'})
            tables  = content.count('CREATE TABLE')
            inserts = content.count('INSERT INTO')
            checks.append({'name': 'SQL content inside gzip',
                           'ok': tables > 0 or inserts > 0,
                           'detail': f'~{tables} tables, ~{inserts} inserts (first 1MB)'})
        except Exception as e:
            checks.append({'name': 'GZip integrity', 'ok': False, 'detail': str(e)})

        return checks, tables, inserts

    # ── Tar ───────────────────────────────────────────────────────────────────

    def _verify_tar(self, path: str):
        checks = []
        try:
            with tarfile.open(path, 'r') as tf:
                members = tf.getnames()
            checks.append({'name': 'Tar archive readable', 'ok': True,
                           'detail': f'{len(members)} members'})
            has_toc = any('toc' in m.lower() or 'restore' in m.lower()
                          for m in members)
            checks.append({'name': 'pg_dump toc.dat present',
                           'ok': has_toc,
                           'detail': 'toc.dat found' if has_toc else 'Not a pg_dump tar'})
        except Exception as e:
            checks.append({'name': 'Tar archive', 'ok': False, 'detail': str(e)})

        return checks

    # ── Binary / custom pg_dump ───────────────────────────────────────────────

    def _verify_binary(self, path: str, db_type: str):
        checks = []
        try:
            with open(path, 'rb') as f:
                magic = f.read(5)

            # pg_dump custom format magic: "PGDMP"
            if db_type == 'postgresql':
                ok = magic == b'PGDMP'
                checks.append({'name': 'PostgreSQL dump magic bytes',
                               'ok': ok,
                               'detail': 'Valid PGDMP header' if ok else f'Got: {magic!r}'})
            else:
                checks.append({'name': 'Binary file non-empty',
                               'ok': len(magic) > 0,
                               'detail': f'First bytes: {magic.hex()}'})

            # Try pg_restore --list if available
            try:
                result = subprocess.run(
                    ['pg_restore', '--list', path],
                    capture_output=True, text=True, timeout=30
                )
                obj_count = len([l for l in result.stdout.splitlines()
                                 if l.strip() and not l.startswith(';')])
                checks.append({'name': 'pg_restore --list succeeded',
                               'ok': result.returncode == 0,
                               'detail': f'{obj_count} objects listed'})
            except (FileNotFoundError, subprocess.TimeoutExpired):
                checks.append({'name': 'pg_restore available locally',
                               'ok': False,
                               'detail': 'pg_restore not found — skipped'})

        except Exception as e:
            checks.append({'name': 'Binary check', 'ok': False, 'detail': str(e)})

        return checks

    # ── Directory (pg_dump --format=directory) ────────────────────────────────

    def _verify_directory(self, path: str):
        checks = []
        tables  = 0
        inserts = 0

        try:
            files = os.listdir(path)
            has_toc = 'toc.dat' in files
            dat_files = [f for f in files if f.endswith('.dat') or f.endswith('.dat.gz')]

            checks.append({'name': 'toc.dat present',
                           'ok': has_toc,
                           'detail': 'Found' if has_toc else 'Missing — invalid dump directory'})
            tables = len(dat_files)
            checks.append({'name': 'Data files present',
                           'ok': len(dat_files) > 0,
                           'detail': f'{len(dat_files)} .dat files'})
        except Exception as e:
            checks.append({'name': 'Directory check', 'ok': False, 'detail': str(e)})

        return checks, tables, inserts

    # ── helpers ───────────────────────────────────────────────────────────────

    def _dir_size(self, path: str) -> int:
        total = 0
        for dirpath, _, filenames in os.walk(path):
            for f in filenames:
                try:
                    total += os.path.getsize(os.path.join(dirpath, f))
                except OSError:
                    pass
        return total

    def _fmt(self, b: int) -> str:
        for unit in ('B', 'KB', 'MB', 'GB'):
            if b < 1024:
                return f'{b:.1f} {unit}'
            b /= 1024
        return f'{b:.2f} TB'
