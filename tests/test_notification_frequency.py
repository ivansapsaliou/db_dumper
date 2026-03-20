"""Tests for notification frequency (per_dump / daily_digest) feature."""

import sys
import os
import json
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from notifier import NotificationManager, _fmt_size, _fmt_duration
from config_manager import ConfigManager


# ── Helpers ───────────────────────────────────────────────────────────────────

@pytest.fixture
def cm(tmp_path):
    return ConfigManager(str(tmp_path / 'config.json'))


def _make_settings(email_freq='per_dump', tg_freq='per_dump'):
    return {
        'notifications': {
            'enabled': True,
            'email': {
                'enabled': True,
                'smtp_host': 'smtp.example.com',
                'smtp_port': 587,
                'smtp_user': 'user@example.com',
                'smtp_password': 'secret',
                'to': 'dest@example.com',
                'use_tls': True,
                'frequency': email_freq,
            },
            'telegram': {
                'enabled': True,
                'bot_token': 'tok123',
                'chat_id': '-100456',
                'frequency': tg_freq,
            },
            'webhook': {'enabled': False, 'url': ''},
        }
    }


def _dump_info(event='success'):
    return {
        'db_name': 'mydb',
        'db_host': 'db.example.com',
        'db_type': 'postgresql',
        'filename': 'mydb_20240101_120000.sql',
        'size': 1024 * 1024 * 10,
        'uncompressed_size': 1024 * 1024 * 20,
        'duration_s': 12.5,
        'speed_mbps': 1.6,
        'rows_exported': 5000,
        'tables_exported': 8,
        'compression_method': 'gzip',
        'compression_ratio': 2.0,
        'finished_at': '2024-01-01T12:00:00',
        'cloud_url': '',
        'message': 'Test error message' if event == 'error' else '',
    }


# ── ConfigManager digest queue ────────────────────────────────────────────────

def test_digest_queue_empty_by_default(cm):
    assert cm.get_digest_queue() == []


def test_add_and_get_digest_queue(cm):
    cm.add_to_digest_queue({'channel': 'email', 'event': 'success', 'dump_info': {}})
    q = cm.get_digest_queue()
    assert len(q) == 1
    assert q[0]['channel'] == 'email'


def test_clear_digest_queue(cm):
    cm.add_to_digest_queue({'channel': 'email', 'event': 'success', 'dump_info': {}})
    cm.add_to_digest_queue({'channel': 'telegram', 'event': 'error', 'dump_info': {}})
    cm.clear_digest_queue()
    assert cm.get_digest_queue() == []


def test_digest_queue_persists_across_instances(tmp_path):
    path = str(tmp_path / 'config.json')
    cm1 = ConfigManager(path)
    cm1.add_to_digest_queue({'channel': 'email', 'event': 'success', 'dump_info': {}})
    cm2 = ConfigManager(path)
    assert len(cm2.get_digest_queue()) == 1


# ── Default config includes frequency field ──────────────────────────────────

def test_default_config_has_email_frequency():
    from config_manager import DEFAULT_CONFIG
    email = DEFAULT_CONFIG['settings']['notifications']['email']
    assert email.get('frequency') == 'per_dump'


def test_default_config_has_telegram_frequency():
    from config_manager import DEFAULT_CONFIG
    tg = DEFAULT_CONFIG['settings']['notifications']['telegram']
    assert tg.get('frequency') == 'per_dump'


# ── NotificationManager per_dump mode (default) ───────────────────────────────

def test_per_dump_sends_email_immediately(cm):
    settings = _make_settings(email_freq='per_dump', tg_freq='per_dump')
    nm = NotificationManager(settings, digest_queue_mgr=cm)
    with patch.object(nm, '_send_email') as mock_email, \
         patch.object(nm, '_send_telegram') as mock_tg:
        nm.notify('success', _dump_info())
        mock_email.assert_called_once()
        mock_tg.assert_called_once()
    # Nothing queued
    assert cm.get_digest_queue() == []


def test_per_dump_sends_error_immediately(cm):
    settings = _make_settings(email_freq='per_dump', tg_freq='per_dump')
    nm = NotificationManager(settings, digest_queue_mgr=cm)
    with patch.object(nm, '_send_email') as mock_email, \
         patch.object(nm, '_send_telegram') as mock_tg:
        nm.notify('error', _dump_info('error'))
        mock_email.assert_called_once()
        mock_tg.assert_called_once()
    assert cm.get_digest_queue() == []


# ── NotificationManager daily_digest mode ────────────────────────────────────

def test_daily_digest_email_queues_not_sends(cm):
    settings = _make_settings(email_freq='daily_digest', tg_freq='per_dump')
    nm = NotificationManager(settings, digest_queue_mgr=cm)
    with patch.object(nm, '_send_email') as mock_email, \
         patch.object(nm, '_send_telegram') as mock_tg:
        nm.notify('success', _dump_info())
        mock_email.assert_not_called()
        mock_tg.assert_called_once()   # telegram is still per_dump
    queue = cm.get_digest_queue()
    assert len(queue) == 1
    assert queue[0]['channel'] == 'email'
    assert queue[0]['event'] == 'success'


def test_daily_digest_telegram_queues_not_sends(cm):
    settings = _make_settings(email_freq='per_dump', tg_freq='daily_digest')
    nm = NotificationManager(settings, digest_queue_mgr=cm)
    with patch.object(nm, '_send_email') as mock_email, \
         patch.object(nm, '_send_telegram') as mock_tg:
        nm.notify('success', _dump_info())
        mock_email.assert_called_once()    # email is still per_dump
        mock_tg.assert_not_called()
    queue = cm.get_digest_queue()
    assert len(queue) == 1
    assert queue[0]['channel'] == 'telegram'


def test_daily_digest_both_queued(cm):
    settings = _make_settings(email_freq='daily_digest', tg_freq='daily_digest')
    nm = NotificationManager(settings, digest_queue_mgr=cm)
    with patch.object(nm, '_send_email'), patch.object(nm, '_send_telegram'):
        nm.notify('success', _dump_info())
        nm.notify('error', _dump_info('error'))
    queue = cm.get_digest_queue()
    assert len(queue) == 4   # 2 events × 2 channels


def test_daily_digest_fallback_when_no_queue_mgr():
    """Without a queue manager, daily_digest should fall back to immediate send."""
    settings = _make_settings(email_freq='daily_digest', tg_freq='daily_digest')
    nm = NotificationManager(settings, digest_queue_mgr=None)
    with patch.object(nm, '_send_email') as mock_email, \
         patch.object(nm, '_send_telegram') as mock_tg:
        nm.notify('success', _dump_info())
        mock_email.assert_called_once()
        mock_tg.assert_called_once()


# ── send_daily_digest ─────────────────────────────────────────────────────────

def test_send_daily_digest_empty_queue_does_nothing(cm):
    settings = _make_settings(email_freq='daily_digest', tg_freq='daily_digest')
    nm = NotificationManager(settings, digest_queue_mgr=cm)
    with patch.object(nm, '_send_email') as mock_email, \
         patch.object(nm, '_send_telegram') as mock_tg:
        nm.send_daily_digest()
        mock_email.assert_not_called()
        mock_tg.assert_not_called()


def test_send_daily_digest_sends_and_clears_queue(cm):
    settings = _make_settings(email_freq='daily_digest', tg_freq='daily_digest')
    nm = NotificationManager(settings, digest_queue_mgr=cm)

    # Pre-populate the queue
    cm.add_to_digest_queue({'channel': 'email', 'event': 'success',
                            'dump_info': _dump_info(), 'queued_at': '2024-01-01T10:00:00'})
    cm.add_to_digest_queue({'channel': 'email', 'event': 'error',
                            'dump_info': _dump_info('error'), 'queued_at': '2024-01-01T11:00:00'})
    cm.add_to_digest_queue({'channel': 'telegram', 'event': 'success',
                            'dump_info': _dump_info(), 'queued_at': '2024-01-01T10:30:00'})

    with patch.object(nm, '_send_email') as mock_email, \
         patch.object(nm, '_send_telegram') as mock_tg:
        nm.send_daily_digest()
        mock_email.assert_called_once()
        mock_tg.assert_called_once()

    # Queue must be cleared
    assert cm.get_digest_queue() == []


def test_send_daily_digest_no_queue_mgr(cm):
    """send_daily_digest with no queue manager is a no-op."""
    settings = _make_settings()
    nm = NotificationManager(settings, digest_queue_mgr=None)
    with patch.object(nm, '_send_email') as mock_email:
        nm.send_daily_digest()
        mock_email.assert_not_called()


def test_send_daily_digest_skips_when_notifications_disabled(cm):
    settings = _make_settings(email_freq='daily_digest', tg_freq='daily_digest')
    settings['notifications']['enabled'] = False
    nm = NotificationManager(settings, digest_queue_mgr=cm)
    cm.add_to_digest_queue({'channel': 'email', 'event': 'success',
                            'dump_info': _dump_info(), 'queued_at': '2024-01-01T10:00:00'})
    with patch.object(nm, '_send_email') as mock_email:
        nm.send_daily_digest()
        mock_email.assert_not_called()
    assert cm.get_digest_queue() == []   # still cleared


# ── Format helpers ────────────────────────────────────────────────────────────

def test_fmt_size():
    assert _fmt_size(0) == '—'
    assert 'KB' in _fmt_size(1024)
    assert 'MB' in _fmt_size(1024 * 1024)
    assert 'GB' in _fmt_size(1024 ** 3)


def test_fmt_duration():
    assert _fmt_duration(0) == '—'
    assert _fmt_duration(45) == '45s'
    assert _fmt_duration(90) == '1m 30s'
    assert _fmt_duration(3661) == '1h 1m 1s'
