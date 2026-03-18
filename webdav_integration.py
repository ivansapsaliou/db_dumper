"""
WebDAVIntegration — upload/download/list dump files via WebDAV protocol.

Uses webdavclient3 (pip install webdavclient3) with a requests fallback.

Configuration keys (from settings['storage']['webdav']):
  enabled       bool
  url           str   — e.g. "https://cloud.example.com/remote.php/dav/files/user/"
  username      str
  password      str
  root_dir      str   — remote path prefix, e.g. "/db-dumps"
  keep_last_n   int   — auto-delete older files (0 = keep all)
"""

import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class WebDAVIntegration:

    def __init__(self, cfg: dict):
        self.cfg = cfg or {}
        self._client = None

    def _get_client(self):
        """Return a webdavclient3 Client instance."""
        if self._client is not None:
            return self._client
        try:
            from webdav3.client import Client
        except ImportError:
            raise RuntimeError(
                'webdavclient3 is required for WebDAV integration '
                '(pip install webdavclient3)'
            )
        options = {
            'webdav_hostname': self.cfg.get('url', '').rstrip('/'),
            'webdav_login':    self.cfg.get('username', ''),
            'webdav_password': self.cfg.get('password', ''),
            'webdav_root':     self.cfg.get('root_dir', '/'),
        }
        self._client = Client(options)
        return self._client

    def _remote_path(self, filename: str) -> str:
        root = self.cfg.get('root_dir', '/').rstrip('/')
        return f'{root}/{filename}'

    def test_connection(self) -> tuple[bool, str]:
        try:
            client = self._get_client()
            root = self.cfg.get('root_dir', '/').rstrip('/')
            if root and not client.check(root):
                client.mkdir(root)
            return True, f'Connected to WebDAV at {self.cfg.get("url")}'
        except Exception as e:
            return False, str(e)

    def upload_file(self, local_path: str, remote_filename: str | None = None) -> dict:
        """
        Upload a local file to WebDAV.
        Returns {'ok': bool, 'remote_path': str, 'message': str}
        """
        if remote_filename is None:
            remote_filename = os.path.basename(local_path)

        remote = self._remote_path(remote_filename)
        try:
            client = self._get_client()
            root = self.cfg.get('root_dir', '/').rstrip('/')
            if root and not client.check(root):
                client.mkdir(root)
            client.upload_sync(remote_path=remote, local_path=local_path)
            logger.info(f'WebDAV upload: {local_path} → {remote}')
            return {'ok': True, 'remote_path': remote, 'message': 'Upload successful'}
        except Exception as e:
            logger.error(f'WebDAV upload failed: {e}')
            return {'ok': False, 'remote_path': remote, 'message': str(e)}

    def list_files(self, path: str | None = None) -> list[dict]:
        """List files in the configured root directory."""
        remote_path = path or self.cfg.get('root_dir', '/')
        try:
            client = self._get_client()
            items = client.list(remote_path)
            result = []
            for name in items:
                if name.endswith('/') or name == os.path.basename(remote_path.rstrip('/')) + '/':
                    continue
                try:
                    info = client.info(f'{remote_path.rstrip("/")}/{name}')
                    result.append({
                        'name': name,
                        'size': int(info.get('size', 0)),
                        'modified': info.get('modified', ''),
                        'path': f'{remote_path.rstrip("/")}/{name}',
                    })
                except Exception:
                    result.append({'name': name, 'size': 0, 'modified': '', 'path': f'{remote_path}/{name}'})
            return sorted(result, key=lambda x: x['modified'], reverse=True)
        except Exception as e:
            logger.error(f'WebDAV list_files failed: {e}')
            return []

    def delete_file(self, remote_path: str) -> bool:
        try:
            self._get_client().clean(remote_path)
            logger.info(f'WebDAV deleted: {remote_path}')
            return True
        except Exception as e:
            logger.error(f'WebDAV delete failed ({remote_path}): {e}')
            return False

    def download_file(self, remote_filename: str, local_path: str) -> bool:
        remote = self._remote_path(remote_filename)
        try:
            self._get_client().download_sync(remote_path=remote, local_path=local_path)
            return True
        except Exception as e:
            logger.error(f'WebDAV download failed: {e}')
            return False

    def apply_retention(self, keep_last_n: int | None = None) -> list[str]:
        n = keep_last_n if keep_last_n is not None else self.cfg.get('keep_last_n', 0)
        if not n or n <= 0:
            return []
        files = self.list_files()
        to_delete = files[n:]
        deleted = []
        for f in to_delete:
            if self.delete_file(f['path']):
                deleted.append(f['name'])
        return deleted
