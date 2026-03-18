"""
RetentionManager — enforces per-database dump retention policies.

Policies (can combine):
  keep_last_n  : keep only the N most recent dumps per DB
  keep_days    : delete dumps older than N days
"""

import os
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class RetentionManager:
    def __init__(self, config_manager):
        self.cm = config_manager

    def apply(self, db_id: str | None = None) -> list[dict]:
        """
        Apply retention policy for one or all databases.
        Returns list of deleted items: [{dump_id, filename, reason}]
        """
        settings = self.cm.get_settings()
        retention = settings.get('retention', {})

        if not retention.get('enabled'):
            return []

        history    = self.cm.get_history()
        deleted    = []

        # Group by db_id
        by_db: dict[str, list] = {}
        for item in history:
            did = item.get('db_id', '__unknown__')
            by_db.setdefault(did, []).append(item)

        targets = [db_id] if db_id else list(by_db.keys())

        for did in targets:
            items = by_db.get(did, [])
            # Only keep 'done' items for deletion consideration
            done_items = [i for i in items if i.get('status') == 'done']
            # Sort newest first
            done_items.sort(key=lambda x: x.get('created_at', ''), reverse=True)

            to_delete_ids = set()

            # Policy: keep last N
            keep_last = retention.get('keep_last_n', 0)
            if keep_last and keep_last > 0:
                for old in done_items[keep_last:]:
                    to_delete_ids.add(old['dump_id'])

            # Policy: keep days
            keep_days = retention.get('keep_days', 0)
            if keep_days and keep_days > 0:
                cutoff = datetime.now() - timedelta(days=keep_days)
                for item in done_items:
                    try:
                        created = datetime.fromisoformat(item['created_at'])
                        if created < cutoff:
                            to_delete_ids.add(item['dump_id'])
                    except Exception:
                        pass

            for item in done_items:
                if item['dump_id'] not in to_delete_ids:
                    continue

                filepath = item.get('filepath', '')
                reason   = []

                if keep_last and keep_last > 0 and done_items.index(item) >= keep_last:
                    reason.append(f'exceeds keep_last={keep_last}')
                if keep_days and keep_days > 0:
                    reason.append(f'older than {keep_days} days')

                # Delete file
                if filepath and os.path.exists(filepath):
                    try:
                        if os.path.isdir(filepath):
                            import shutil
                            shutil.rmtree(filepath)
                        else:
                            os.remove(filepath)
                        logger.info(f'Retention: deleted {filepath}')
                    except Exception as e:
                        logger.warning(f'Retention: could not delete {filepath}: {e}')

                # Remove from history
                self.cm.delete_history(item['dump_id'])

                deleted.append({
                    'dump_id':  item['dump_id'],
                    'filename': item.get('filename', '?'),
                    'reason':   ', '.join(reason),
                })

        if deleted:
            logger.info(f'Retention: removed {len(deleted)} dump(s)')

        return deleted

    def preview(self, db_id: str | None = None) -> list[dict]:
        """
        Return what WOULD be deleted without actually deleting.
        """
        settings = self.cm.get_settings()
        retention = settings.get('retention', {})
        if not retention.get('enabled'):
            return []

        history = self.cm.get_history()
        by_db: dict[str, list] = {}
        for item in history:
            did = item.get('db_id', '__unknown__')
            by_db.setdefault(did, []).append(item)

        targets = [db_id] if db_id else list(by_db.keys())
        would_delete = []

        for did in targets:
            items = by_db.get(did, [])
            done_items = [i for i in items if i.get('status') == 'done']
            done_items.sort(key=lambda x: x.get('created_at', ''), reverse=True)

            keep_last = retention.get('keep_last_n', 0)
            keep_days = retention.get('keep_days', 0)

            for idx, item in enumerate(done_items):
                reasons = []
                if keep_last and idx >= keep_last:
                    reasons.append(f'exceeds keep_last={keep_last}')
                if keep_days:
                    try:
                        created = datetime.fromisoformat(item['created_at'])
                        if datetime.now() - created > timedelta(days=keep_days):
                            reasons.append(f'older than {keep_days}d')
                    except Exception:
                        pass
                if reasons:
                    would_delete.append({
                        'dump_id':  item['dump_id'],
                        'filename': item.get('filename', '?'),
                        'db_name':  item.get('db_name', '?'),
                        'created_at': item.get('created_at', ''),
                        'size':     item.get('size', 0),
                        'reason':   ', '.join(reasons),
                    })

        return would_delete
