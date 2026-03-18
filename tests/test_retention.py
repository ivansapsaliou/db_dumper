"""Unit tests for RetentionManager."""

import sys
import os
import pytest
import tempfile
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config_manager import ConfigManager
from retention import RetentionManager


@pytest.fixture
def cm(tmp_path):
    return ConfigManager(str(tmp_path / 'config.json'))


@pytest.fixture
def mgr(cm):
    return RetentionManager(cm)


def _make_file(tmp_path, name: str) -> str:
    p = tmp_path / name
    p.write_text('-- dump')
    return str(p)


def _add_history(cm, dump_id, filename, filepath, age_days=0, status='done'):
    created = (datetime.now() - timedelta(days=age_days)).isoformat()
    cm.add_history({
        'dump_id':    dump_id,
        'db_id':      'db-1',
        'db_name':    'testdb',
        'filename':   filename,
        'filepath':   filepath,
        'size':       1024,
        'status':     status,
        'created_at': created,
    })


def test_retention_disabled_does_nothing(cm, mgr, tmp_path):
    settings = cm.get_settings()
    settings['retention'] = {'enabled': False, 'keep_last_n': 1, 'keep_days': 1}
    cm.save_settings(settings)
    fp = _make_file(tmp_path, 'old.sql')
    _add_history(cm, 'd-1', 'old.sql', fp, age_days=100)
    deleted = mgr.apply()
    assert deleted == []
    assert os.path.exists(fp)


def test_keep_last_n(cm, mgr, tmp_path):
    settings = cm.get_settings()
    settings['retention'] = {'enabled': True, 'keep_last_n': 2, 'keep_days': 0}
    cm.save_settings(settings)
    files = [_make_file(tmp_path, f'd{i}.sql') for i in range(4)]
    for i, fp in enumerate(files):
        _add_history(cm, f'd-{i}', f'd{i}.sql', fp, age_days=i)
    deleted = mgr.apply('db-1')
    assert len(deleted) == 2
    # Oldest 2 files should be deleted from disk
    for fp in files[2:]:
        assert not os.path.exists(fp)


def test_keep_days(cm, mgr, tmp_path):
    settings = cm.get_settings()
    settings['retention'] = {'enabled': True, 'keep_last_n': 0, 'keep_days': 7}
    cm.save_settings(settings)
    old_fp = _make_file(tmp_path, 'old.sql')
    new_fp = _make_file(tmp_path, 'new.sql')
    _add_history(cm, 'old', 'old.sql', old_fp, age_days=30)
    _add_history(cm, 'new', 'new.sql', new_fp, age_days=1)
    deleted = mgr.apply('db-1')
    assert len(deleted) == 1
    assert not os.path.exists(old_fp)
    assert os.path.exists(new_fp)


def test_preview_does_not_delete(cm, mgr, tmp_path):
    settings = cm.get_settings()
    settings['retention'] = {'enabled': True, 'keep_last_n': 1, 'keep_days': 0}
    cm.save_settings(settings)
    files = [_make_file(tmp_path, f'd{i}.sql') for i in range(3)]
    for i, fp in enumerate(files):
        _add_history(cm, f'd-{i}', f'd{i}.sql', fp, age_days=i)
    preview = mgr.preview('db-1')
    assert len(preview) == 2
    for fp in files:
        assert os.path.exists(fp)  # files not deleted in preview


def test_non_done_status_not_deleted(cm, mgr, tmp_path):
    settings = cm.get_settings()
    settings['retention'] = {'enabled': True, 'keep_last_n': 0, 'keep_days': 1}
    cm.save_settings(settings)
    fp = _make_file(tmp_path, 'err.sql')
    _add_history(cm, 'e-1', 'err.sql', fp, age_days=30, status='error')
    deleted = mgr.apply('db-1')
    assert deleted == []
    assert os.path.exists(fp)
