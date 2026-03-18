"""Unit tests for ConfigManager."""

import sys
import os
import json
import pytest
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config_manager import ConfigManager, DEFAULT_CONFIG


@pytest.fixture
def cm(tmp_path):
    return ConfigManager(str(tmp_path / 'config.json'))


def test_initial_config_created(tmp_path):
    path = str(tmp_path / 'new_config.json')
    assert not os.path.exists(path)
    ConfigManager(path)
    assert os.path.exists(path)
    with open(path) as f:
        data = json.load(f)
    assert 'databases' in data
    assert 'schedules' in data
    assert 'history' in data
    assert 'settings' in data


def test_add_and_get_database(cm):
    db = {'id': 'db-1', 'name': 'TestDB', 'host': 'localhost', 'type': 'postgresql'}
    cm.add_database(db)
    dbs = cm.get_databases()
    assert len(dbs) == 1
    assert dbs[0]['id'] == 'db-1'


def test_update_database(cm):
    cm.add_database({'id': 'db-1', 'name': 'Old', 'host': 'host1'})
    cm.update_database('db-1', {'name': 'New', 'host': 'host2'})
    db = cm.get_database('db-1')
    assert db['name'] == 'New'
    assert db['host'] == 'host2'


def test_delete_database(cm):
    cm.add_database({'id': 'db-1', 'name': 'A'})
    cm.add_database({'id': 'db-2', 'name': 'B'})
    cm.delete_database('db-1')
    assert cm.get_database('db-1') is None
    assert cm.get_database('db-2') is not None


def test_add_and_get_schedule(cm):
    s = {'id': 's-1', 'cron': '0 2 * * *', 'db_id': 'db-1', 'enabled': True}
    cm.add_schedule(s)
    schedules = cm.get_schedules()
    assert len(schedules) == 1
    assert schedules[0]['id'] == 's-1'


def test_delete_schedule(cm):
    cm.add_schedule({'id': 's-1', 'cron': '0 1 * * *'})
    cm.delete_schedule('s-1')
    assert cm.get_schedules() == []


def test_add_history_insert_order(cm):
    for i in range(3):
        cm.add_history({'dump_id': f'd-{i}', 'db_name': 'test', 'status': 'done',
                        'filename': f'f{i}.sql', 'size': i * 1000, 'created_at': '2024-01-01'})
    h = cm.get_history()
    # newest first
    assert h[0]['dump_id'] == 'd-2'


def test_history_max_limit(cm):
    existing = cm.get_settings()
    existing['max_history'] = 5
    cm.save_settings(existing)
    for i in range(10):
        cm.add_history({'dump_id': f'd-{i}', 'db_name': 'test', 'status': 'done',
                        'filename': f'f{i}.sql', 'size': 0, 'created_at': '2024-01-01'})
    assert len(cm.get_history()) == 5


def test_delete_history(cm):
    cm.add_history({'dump_id': 'x', 'db_name': 'db', 'status': 'done',
                    'filename': 'f.sql', 'size': 0, 'created_at': ''})
    cm.delete_history('x')
    assert all(h['dump_id'] != 'x' for h in cm.get_history())


def test_save_and_get_settings(cm):
    original = cm.get_settings()
    original['default_save_path'] = '/custom/path'
    cm.save_settings(original)
    assert cm.get_settings()['default_save_path'] == '/custom/path'
