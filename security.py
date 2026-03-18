"""
SecurityManager — audit logging, RBAC, and data masking.

Audit log: stored in SQLite (audit.db).
RBAC: three roles — admin, operator, viewer.
DataMasker: replaces PII patterns (email, SSN, phone, credit card) with *****.
"""

import re
import os
import json
import sqlite3
import logging
import hashlib
import secrets
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

AUDIT_DB = os.environ.get('AUDIT_DB', 'audit.db')

# ── RBAC definitions ────────────────────────────────────────────────────────────

ROLES = ('admin', 'operator', 'viewer')

PERMISSIONS = {
    'admin': {
        'databases:read', 'databases:write', 'databases:delete',
        'dumps:start', 'dumps:cancel', 'dumps:download',
        'schedules:read', 'schedules:write', 'schedules:delete',
        'history:read', 'history:delete',
        'settings:read', 'settings:write',
        'audit:read',
        'users:read', 'users:write',
        'storage:read', 'storage:write',
    },
    'operator': {
        'databases:read', 'databases:write',
        'dumps:start', 'dumps:cancel', 'dumps:download',
        'schedules:read', 'schedules:write',
        'history:read',
        'settings:read',
        'storage:read',
    },
    'viewer': {
        'databases:read',
        'dumps:download',
        'schedules:read',
        'history:read',
        'settings:read',
        'storage:read',
    },
}


class RBACManager:
    """Simple role-based access control manager."""

    def __init__(self, users_file: str = 'users.json'):
        self.users_file = users_file
        self._ensure_default_admin()

    def _ensure_default_admin(self):
        """Create default admin user if no users file exists."""
        if not os.path.exists(self.users_file):
            default_pwd = 'admin'
            self._write_users([{
                'username': 'admin',
                'password_hash': self._hash_password(default_pwd),
                'role': 'admin',
                'created_at': datetime.now().isoformat(),
            }])
            logger.info(
                f'Created default admin user (username: admin, password: {default_pwd}). '
                'Change this immediately in production!'
            )

    def _read_users(self) -> list:
        if not os.path.exists(self.users_file):
            return []
        try:
            with open(self.users_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return []

    def _write_users(self, users: list):
        with open(self.users_file, 'w', encoding='utf-8') as f:
            json.dump(users, f, indent=2, ensure_ascii=False)

    def _hash_password(self, password: str) -> str:
        salt = secrets.token_hex(16)
        h = hashlib.sha256((salt + password).encode()).hexdigest()
        return f'{salt}:{h}'

    def _verify_password(self, password: str, stored_hash: str) -> bool:
        try:
            salt, h = stored_hash.split(':', 1)
            return hashlib.sha256((salt + password).encode()).hexdigest() == h
        except Exception:
            return False

    def authenticate(self, username: str, password: str) -> Optional[dict]:
        """Return user dict if credentials valid, else None."""
        for user in self._read_users():
            if user.get('username') == username:
                if self._verify_password(password, user.get('password_hash', '')):
                    return {'username': user['username'], 'role': user.get('role', 'viewer')}
        return None

    def has_permission(self, role: str, permission: str) -> bool:
        return permission in PERMISSIONS.get(role, set())

    def get_users(self) -> list:
        return [
            {'username': u['username'], 'role': u.get('role', 'viewer'),
             'created_at': u.get('created_at', '')}
            for u in self._read_users()
        ]

    def create_user(self, username: str, password: str, role: str) -> bool:
        if role not in ROLES:
            raise ValueError(f'Invalid role: {role}')
        users = self._read_users()
        if any(u['username'] == username for u in users):
            return False
        users.append({
            'username': username,
            'password_hash': self._hash_password(password),
            'role': role,
            'created_at': datetime.now().isoformat(),
        })
        self._write_users(users)
        return True

    def update_user_role(self, username: str, role: str) -> bool:
        if role not in ROLES:
            raise ValueError(f'Invalid role: {role}')
        users = self._read_users()
        for u in users:
            if u['username'] == username:
                u['role'] = role
                self._write_users(users)
                return True
        return False

    def delete_user(self, username: str) -> bool:
        users = self._read_users()
        new = [u for u in users if u['username'] != username]
        if len(new) == len(users):
            return False
        self._write_users(new)
        return True

    def change_password(self, username: str, new_password: str) -> bool:
        users = self._read_users()
        for u in users:
            if u['username'] == username:
                u['password_hash'] = self._hash_password(new_password)
                self._write_users(users)
                return True
        return False


# ── Audit Logger ────────────────────────────────────────────────────────────────

class AuditLogger:
    """Logs all security-relevant operations to a SQLite audit database."""

    def __init__(self, db_path: str = AUDIT_DB):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS audit_log (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts        TEXT NOT NULL,
                    user      TEXT,
                    action    TEXT NOT NULL,
                    resource  TEXT,
                    ip        TEXT,
                    status    TEXT,
                    details   TEXT
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_ts ON audit_log(ts)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_user ON audit_log(user)')
            conn.commit()

    def log(self, action: str, resource: str = None, user: str = 'system',
            ip: str = None, status: str = 'ok', details: str = None):
        ts = datetime.now().isoformat(timespec='seconds')
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    'INSERT INTO audit_log(ts, user, action, resource, ip, status, details) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?)',
                    (ts, user, action, resource, ip, status, details)
                )
                conn.commit()
        except Exception as e:
            logger.error(f'AuditLogger.log failed: {e}')

    def get_logs(self, limit: int = 200, offset: int = 0,
                 user: str = None, action: str = None,
                 since: str = None, until: str = None) -> list[dict]:
        filters = []
        params: list = []
        if user:
            filters.append('user = ?'); params.append(user)
        if action:
            filters.append('action LIKE ?'); params.append(f'%{action}%')
        if since:
            filters.append('ts >= ?'); params.append(since)
        if until:
            filters.append('ts <= ?'); params.append(until)

        where = ('WHERE ' + ' AND '.join(filters)) if filters else ''
        params += [limit, offset]

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    f'SELECT * FROM audit_log {where} ORDER BY id DESC LIMIT ? OFFSET ?',
                    params
                ).fetchall()
                return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f'AuditLogger.get_logs failed: {e}')
            return []

    def get_total(self, user: str = None, action: str = None,
                  since: str = None, until: str = None) -> int:
        filters = []
        params: list = []
        if user:
            filters.append('user = ?'); params.append(user)
        if action:
            filters.append('action LIKE ?'); params.append(f'%{action}%')
        if since:
            filters.append('ts >= ?'); params.append(since)
        if until:
            filters.append('ts <= ?'); params.append(until)

        where = ('WHERE ' + ' AND '.join(filters)) if filters else ''
        try:
            with sqlite3.connect(self.db_path) as conn:
                row = conn.execute(f'SELECT COUNT(*) FROM audit_log {where}', params).fetchone()
                return row[0] if row else 0
        except Exception:
            return 0

    def purge_old(self, keep_days: int = 90) -> int:
        """Delete entries older than keep_days. Returns count of deleted rows."""
        cutoff = datetime.now().isoformat(timespec='seconds')
        from datetime import timedelta
        cutoff_dt = (datetime.now() - timedelta(days=keep_days)).isoformat(timespec='seconds')
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute('DELETE FROM audit_log WHERE ts < ?', (cutoff_dt,))
                conn.commit()
                return cursor.rowcount
        except Exception as e:
            logger.error(f'AuditLogger.purge_old failed: {e}')
            return 0


# ── Data Masker ─────────────────────────────────────────────────────────────────

# PII patterns
_EMAIL_RE   = re.compile(r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b')
_PHONE_RE   = re.compile(r'\b(?:\+?[\d\-\(\)\s]{7,15})\b')
_SSN_RE     = re.compile(r'\b\d{3}[-\s]\d{2}[-\s]\d{4}\b')
_CARD_RE    = re.compile(r'\b(?:\d{4}[-\s]?){3}\d{4}\b')
_IP_RE      = re.compile(r'\b(?:\d{1,3}\.){3}\d{1,3}\b')


class DataMasker:
    """Mask PII patterns in SQL dump text."""

    def __init__(self, mask_char: str = '*', mask_len: int = 8):
        self.mask = mask_char * mask_len

    def mask_line(self, line: str, patterns: set | None = None) -> str:
        """Mask PII in a single line of text."""
        if patterns is None:
            patterns = {'email', 'ssn', 'card'}  # phone and IP off by default (too many false positives)
        if 'email' in patterns:
            line = _EMAIL_RE.sub(self.mask, line)
        if 'ssn' in patterns:
            line = _SSN_RE.sub(self.mask, line)
        if 'card' in patterns:
            line = _CARD_RE.sub(self.mask, line)
        if 'phone' in patterns:
            line = _PHONE_RE.sub(self.mask, line)
        if 'ip' in patterns:
            line = _IP_RE.sub(self.mask, line)
        return line

    def mask_file(self, src_path: str, dst_path: str, patterns: set | None = None) -> int:
        """
        Process a SQL dump file, masking PII in each line.
        Returns number of lines modified.
        """
        modified = 0
        with open(src_path, 'r', encoding='utf-8', errors='replace') as fin, \
             open(dst_path, 'w', encoding='utf-8') as fout:
            for line in fin:
                masked = self.mask_line(line, patterns)
                if masked != line:
                    modified += 1
                fout.write(masked)
        logger.info(f'DataMasker: masked {modified} lines in {os.path.basename(src_path)}')
        return modified


# ── Singletons ──────────────────────────────────────────────────────────────────

_audit_logger: AuditLogger | None = None
_rbac_manager: RBACManager | None = None
_data_masker: DataMasker | None = None


def get_audit_logger() -> AuditLogger:
    global _audit_logger
    if _audit_logger is None:
        _audit_logger = AuditLogger()
    return _audit_logger


def get_rbac_manager() -> RBACManager:
    global _rbac_manager
    if _rbac_manager is None:
        _rbac_manager = RBACManager()
    return _rbac_manager


def get_data_masker() -> DataMasker:
    global _data_masker
    if _data_masker is None:
        _data_masker = DataMasker()
    return _data_masker
