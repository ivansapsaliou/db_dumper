"""
S3Integration — upload/download dump files to Amazon S3 or MinIO-compatible storage.

Requires: boto3 (pip install boto3)
"""

import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class S3Integration:
    """
    Handles upload, listing and deletion of dump files in S3/MinIO.

    Configuration keys (from settings['storage']['s3']):
      enabled         bool
      bucket          str   — S3 bucket name
      prefix          str   — key prefix, e.g. "db-dumps/"
      region          str   — AWS region or "" for MinIO
      endpoint_url    str   — custom endpoint for MinIO, e.g. "http://minio:9000"
      access_key      str
      secret_key      str
      keep_last_n     int   — auto-delete older keys (0 = keep all)
    """

    def __init__(self, cfg: dict):
        self.cfg = cfg or {}
        self._client = None

    def _get_client(self):
        if self._client is not None:
            return self._client
        try:
            import boto3
            from botocore.client import Config
        except ImportError:
            raise RuntimeError('boto3 is required for S3 integration (pip install boto3)')

        kwargs: dict = {
            'aws_access_key_id':     self.cfg.get('access_key') or None,
            'aws_secret_access_key': self.cfg.get('secret_key') or None,
            'region_name':           self.cfg.get('region') or None,
        }
        endpoint = self.cfg.get('endpoint_url', '').strip()
        if endpoint:
            kwargs['endpoint_url'] = endpoint
            kwargs['config'] = Config(signature_version='s3v4')

        self._client = boto3.client('s3', **kwargs)
        return self._client

    def test_connection(self) -> tuple[bool, str]:
        """Return (ok, message)."""
        try:
            client = self._get_client()
            bucket = self.cfg.get('bucket', '')
            if not bucket:
                return False, 'Bucket name is required'
            client.head_bucket(Bucket=bucket)
            return True, f'Connected to bucket "{bucket}"'
        except Exception as e:
            return False, str(e)

    def upload_file(self, local_path: str, remote_key: str | None = None) -> dict:
        """
        Upload a local file to S3.
        Returns {'ok': bool, 'key': str, 'url': str, 'message': str}
        """
        bucket = self.cfg.get('bucket', '')
        prefix = self.cfg.get('prefix', '').rstrip('/')
        if not bucket:
            return {'ok': False, 'message': 'S3 bucket not configured'}

        if remote_key is None:
            filename = os.path.basename(local_path)
            remote_key = f'{prefix}/{filename}' if prefix else filename

        try:
            client = self._get_client()
            file_size = os.path.getsize(local_path)

            extra = {'Metadata': {
                'uploaded-at': datetime.now().isoformat(),
                'original-name': os.path.basename(local_path),
                'size-bytes': str(file_size),
            }}

            client.upload_file(local_path, bucket, remote_key, ExtraArgs=extra)

            endpoint = self.cfg.get('endpoint_url', '').strip()
            if endpoint:
                url = f"{endpoint.rstrip('/')}/{bucket}/{remote_key}"
            else:
                region = self.cfg.get('region', 'us-east-1')
                url = f'https://{bucket}.s3.{region}.amazonaws.com/{remote_key}'

            logger.info(f'S3 upload: {local_path} → s3://{bucket}/{remote_key}')
            return {'ok': True, 'key': remote_key, 'url': url, 'message': 'Upload successful'}

        except Exception as e:
            logger.error(f'S3 upload failed: {e}')
            return {'ok': False, 'key': remote_key, 'url': '', 'message': str(e)}

    def list_objects(self, prefix: str | None = None) -> list[dict]:
        """List objects in the bucket under the configured prefix."""
        bucket = self.cfg.get('bucket', '')
        if not bucket:
            return []
        pfx = prefix if prefix is not None else self.cfg.get('prefix', '')

        try:
            client = self._get_client()
            paginator = client.get_paginator('list_objects_v2')
            objects = []
            for page in paginator.paginate(Bucket=bucket, Prefix=pfx):
                for obj in page.get('Contents', []):
                    objects.append({
                        'key':          obj['Key'],
                        'size':         obj['Size'],
                        'last_modified': obj['LastModified'].isoformat(),
                        'etag':         obj.get('ETag', '').strip('"'),
                    })
            return sorted(objects, key=lambda x: x['last_modified'], reverse=True)
        except Exception as e:
            logger.error(f'S3 list_objects failed: {e}')
            return []

    def delete_object(self, key: str) -> bool:
        """Delete a single object by key."""
        bucket = self.cfg.get('bucket', '')
        try:
            self._get_client().delete_object(Bucket=bucket, Key=key)
            logger.info(f'S3 deleted: s3://{bucket}/{key}')
            return True
        except Exception as e:
            logger.error(f'S3 delete failed ({key}): {e}')
            return False

    def apply_retention(self, keep_last_n: int | None = None) -> list[str]:
        """
        Delete old objects, keeping only the N most recent.
        Returns list of deleted keys.
        """
        n = keep_last_n if keep_last_n is not None else self.cfg.get('keep_last_n', 0)
        if not n or n <= 0:
            return []
        objects = self.list_objects()
        to_delete = objects[n:]
        deleted = []
        for obj in to_delete:
            if self.delete_object(obj['key']):
                deleted.append(obj['key'])
        return deleted

    def generate_presigned_url(self, key: str, expires_in: int = 3600) -> str | None:
        """Generate a presigned download URL valid for expires_in seconds."""
        bucket = self.cfg.get('bucket', '')
        try:
            url = self._get_client().generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket, 'Key': key},
                ExpiresIn=expires_in,
            )
            return url
        except Exception as e:
            logger.error(f'S3 presigned URL failed: {e}')
            return None
