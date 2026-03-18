"""
CryptoManager — encrypts/decrypts sensitive fields in config.json
using Fernet symmetric encryption. Key is stored in .secret.key
(should never be committed to VCS).
"""

import os
import base64
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

SENSITIVE_FIELDS = ('password', 'ssh_password')
KEY_FILE = '.secret.key'
ENC_PREFIX = 'ENC:'


class CryptoManager:
    def __init__(self, key_path: str = KEY_FILE):
        self.key_path = key_path
        self._fernet = None
        self._load_or_create_key()

    def _load_or_create_key(self):
        try:
            from cryptography.fernet import Fernet
            if os.path.exists(self.key_path):
                with open(self.key_path, 'rb') as f:
                    key = f.read().strip()
            else:
                key = Fernet.generate_key()
                with open(self.key_path, 'wb') as f:
                    f.write(key)
                logger.info(f'Generated new encryption key: {self.key_path}')
            self._fernet = Fernet(key)
        except ImportError:
            logger.warning('cryptography not installed — passwords stored in plaintext')
            self._fernet = None
        except Exception as e:
            logger.error(f'CryptoManager init failed: {e} — falling back to plaintext')
            self._fernet = None

    def encrypt(self, value: str) -> str:
        if not self._fernet or not value:
            return value
        try:
            encrypted = self._fernet.encrypt(value.encode()).decode()
            return ENC_PREFIX + encrypted
        except Exception:
            return value

    def decrypt(self, value: str) -> str:
        if not self._fernet or not value:
            return value
        if not str(value).startswith(ENC_PREFIX):
            return value  # plaintext (legacy or unencrypted)
        try:
            raw = value[len(ENC_PREFIX):]
            return self._fernet.decrypt(raw.encode()).decode()
        except Exception:
            logger.warning('Failed to decrypt value — returning as-is')
            return value

    def encrypt_db_config(self, cfg: dict) -> dict:
        """Return a copy of cfg with sensitive fields encrypted."""
        result = dict(cfg)
        for field in SENSITIVE_FIELDS:
            if field in result and result[field]:
                result[field] = self.encrypt(str(result[field]))
        return result

    def decrypt_db_config(self, cfg: dict) -> dict:
        """Return a copy of cfg with sensitive fields decrypted."""
        result = dict(cfg)
        for field in SENSITIVE_FIELDS:
            if field in result and result[field]:
                result[field] = self.decrypt(str(result[field]))
        return result

    def is_available(self) -> bool:
        return self._fernet is not None


# Singleton
_instance = None

def get_crypto() -> CryptoManager:
    global _instance
    if _instance is None:
        _instance = CryptoManager()
    return _instance
