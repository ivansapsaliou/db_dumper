"""Unit tests for SecurityManager (AuditLogger, RBACManager, DataMasker)."""

import sys
import os
import pytest
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from security import AuditLogger, RBACManager, DataMasker, PERMISSIONS


# ── AuditLogger ───────────────────────────────────────────────────────────────

@pytest.fixture
def audit(tmp_path):
    return AuditLogger(db_path=str(tmp_path / 'audit_test.db'))


def test_audit_log_basic(audit):
    audit.log('dump_start', resource='mydb', user='admin', status='ok')
    logs = audit.get_logs()
    assert len(logs) == 1
    assert logs[0]['action'] == 'dump_start'
    assert logs[0]['user'] == 'admin'
    assert logs[0]['status'] == 'ok'


def test_audit_log_multiple(audit):
    for i in range(5):
        audit.log(f'action_{i}', user='user1')
    assert audit.get_total() == 5


def test_audit_filter_by_user(audit):
    audit.log('op1', user='alice')
    audit.log('op2', user='bob')
    audit.log('op3', user='alice')
    logs = audit.get_logs(user='alice')
    assert len(logs) == 2
    assert all(l['user'] == 'alice' for l in logs)


def test_audit_filter_by_action(audit):
    audit.log('dump_success', user='u1')
    audit.log('login', user='u1')
    audit.log('dump_error', user='u1')
    logs = audit.get_logs(action='dump')
    assert len(logs) == 2


def test_audit_purge(audit):
    from datetime import datetime, timedelta
    # Log with an old timestamp by inserting directly
    import sqlite3
    old_ts = (datetime.now() - timedelta(days=100)).isoformat(timespec='seconds')
    with sqlite3.connect(audit.db_path) as conn:
        conn.execute(
            'INSERT INTO audit_log(ts, user, action, status) VALUES (?,?,?,?)',
            (old_ts, 'sys', 'old_event', 'ok')
        )
        conn.commit()
    audit.log('recent_event', user='sys')
    deleted = audit.purge_old(keep_days=30)
    assert deleted == 1
    assert audit.get_total() == 1


# ── RBACManager ───────────────────────────────────────────────────────────────

@pytest.fixture
def rbac(tmp_path):
    return RBACManager(users_file=str(tmp_path / 'users.json'))


def test_rbac_default_admin_created(tmp_path):
    mgr = RBACManager(users_file=str(tmp_path / 'u.json'))
    users = mgr.get_users()
    assert any(u['username'] == 'admin' and u['role'] == 'admin' for u in users)


def test_rbac_authenticate_success(rbac):
    user = rbac.authenticate('admin', 'admin')
    assert user is not None
    assert user['role'] == 'admin'


def test_rbac_authenticate_wrong_password(rbac):
    assert rbac.authenticate('admin', 'wrong') is None


def test_rbac_create_and_authenticate_user(rbac):
    ok = rbac.create_user('operator1', 'pass123', 'operator')
    assert ok
    user = rbac.authenticate('operator1', 'pass123')
    assert user is not None
    assert user['role'] == 'operator'


def test_rbac_duplicate_user(rbac):
    rbac.create_user('dup', 'p', 'viewer')
    assert not rbac.create_user('dup', 'p2', 'viewer')


def test_rbac_invalid_role(rbac):
    with pytest.raises(ValueError):
        rbac.create_user('bad', 'p', 'superadmin')


def test_rbac_has_permission_admin(rbac):
    assert rbac.has_permission('admin', 'databases:write')
    assert rbac.has_permission('admin', 'audit:read')


def test_rbac_has_permission_viewer(rbac):
    assert rbac.has_permission('viewer', 'databases:read')
    assert not rbac.has_permission('viewer', 'databases:write')
    assert not rbac.has_permission('viewer', 'audit:read')


def test_rbac_update_role(rbac):
    rbac.create_user('testuser', 'p', 'viewer')
    assert rbac.update_user_role('testuser', 'operator')
    user = rbac.authenticate('testuser', 'p')
    assert user['role'] == 'operator'


def test_rbac_delete_user(rbac):
    rbac.create_user('todel', 'p', 'viewer')
    assert rbac.delete_user('todel')
    assert rbac.authenticate('todel', 'p') is None


def test_rbac_change_password(rbac):
    rbac.create_user('u', 'old', 'viewer')
    rbac.change_password('u', 'new')
    assert rbac.authenticate('u', 'new') is not None
    assert rbac.authenticate('u', 'old') is None


# ── DataMasker ────────────────────────────────────────────────────────────────

@pytest.fixture
def masker():
    return DataMasker()


def test_mask_email(masker):
    line = "INSERT INTO users VALUES (1, 'John', 'john@example.com', 30);"
    masked = masker.mask_line(line, {'email'})
    assert 'john@example.com' not in masked
    assert masker.mask in masked


def test_mask_ssn(masker):
    line = "INSERT INTO records VALUES (1, '123-45-6789');"
    masked = masker.mask_line(line, {'ssn'})
    assert '123-45-6789' not in masked


def test_mask_credit_card(masker):
    line = "INSERT INTO orders VALUES (1, '4111 1111 1111 1111', 100);"
    masked = masker.mask_line(line, {'card'})
    assert '4111' not in masked


def test_mask_file(masker, tmp_path):
    src = tmp_path / 'dump.sql'
    src.write_text("INSERT INTO users VALUES (1, 'alice@test.com');\n"
                   "INSERT INTO users VALUES (2, 'no_email_here');\n")
    dst = tmp_path / 'masked.sql'
    count = masker.mask_file(str(src), str(dst), {'email'})
    assert count == 1
    content = dst.read_text()
    assert 'alice@test.com' not in content
    assert 'no_email_here' in content
