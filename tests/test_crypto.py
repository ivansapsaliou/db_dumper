"""Unit tests for CryptoManager."""

import sys
import os
import tempfile
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from crypto_manager import CryptoManager, ENC_PREFIX


@pytest.fixture
def crypto(tmp_path):
    key_file = str(tmp_path / 'test.key')
    return CryptoManager(key_file)


def test_encrypt_decrypt_roundtrip(crypto):
    original = 'super_secret_password_123'
    enc = crypto.encrypt(original)
    assert enc.startswith(ENC_PREFIX)
    assert enc != original
    dec = crypto.decrypt(enc)
    assert dec == original


def test_encrypt_empty_string(crypto):
    assert crypto.encrypt('') == ''


def test_decrypt_plaintext_passthrough(crypto):
    # Values without ENC: prefix are returned as-is (legacy support)
    plain = 'not_encrypted'
    assert crypto.decrypt(plain) == plain


def test_encrypt_db_config(crypto):
    cfg = {
        'id': '1',
        'host': 'localhost',
        'password': 'secret',
        'ssh_password': 'key_passphrase',
    }
    enc = crypto.encrypt_db_config(cfg)
    assert enc['password'].startswith(ENC_PREFIX)
    assert enc['ssh_password'].startswith(ENC_PREFIX)
    assert enc['host'] == 'localhost'  # non-sensitive not changed


def test_decrypt_db_config(crypto):
    cfg = {
        'id': '1',
        'host': 'dbhost',
        'password': 'my_pass',
        'ssh_password': 'my_ssh',
    }
    enc = crypto.encrypt_db_config(cfg)
    dec = crypto.decrypt_db_config(enc)
    assert dec['password'] == 'my_pass'
    assert dec['ssh_password'] == 'my_ssh'
    assert dec['host'] == 'dbhost'


def test_is_available(crypto):
    # If cryptography is installed it should be available
    try:
        from cryptography.fernet import Fernet
        assert crypto.is_available() is True
    except ImportError:
        assert crypto.is_available() is False


def test_key_persistence(tmp_path):
    key_file = str(tmp_path / 'persist.key')
    c1 = CryptoManager(key_file)
    enc = c1.encrypt('hello')
    c2 = CryptoManager(key_file)  # second instance reads same key
    assert c2.decrypt(enc) == 'hello'
