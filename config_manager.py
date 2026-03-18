import json
import os
from threading import Lock

_lock = Lock()

DEFAULT_CONFIG = {
    "databases": [],
    "schedules": [],
    "history": [],
    "settings": {
        "default_save_path": "./dumps",
        "max_history": 100
    }
}


class ConfigManager:
    def __init__(self, path: str):
        self.path = path
        if not os.path.exists(path):
            self._write(DEFAULT_CONFIG)

    def _read(self) -> dict:
        with _lock:
            with open(self.path, 'r', encoding='utf-8') as f:
                return json.load(f)

    def _write(self, data: dict):
        with _lock:
            with open(self.path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

    # Databases
    def get_databases(self):
        return self._read().get('databases', [])

    def get_database(self, db_id):
        for db in self.get_databases():
            if db.get('id') == db_id:
                return db
        return None

    def add_database(self, db):
        cfg = self._read()
        cfg['databases'].append(db)
        self._write(cfg)

    def update_database(self, db_id, data):
        cfg = self._read()
        for i, db in enumerate(cfg['databases']):
            if db.get('id') == db_id:
                data['id'] = db_id
                cfg['databases'][i] = data
                break
        self._write(cfg)

    def delete_database(self, db_id):
        cfg = self._read()
        cfg['databases'] = [d for d in cfg['databases'] if d.get('id') != db_id]
        self._write(cfg)

    # Schedules
    def get_schedules(self):
        return self._read().get('schedules', [])

    def add_schedule(self, s):
        cfg = self._read()
        cfg['schedules'].append(s)
        self._write(cfg)

    def save_schedules(self, schedules):
        cfg = self._read()
        cfg['schedules'] = schedules
        self._write(cfg)

    def delete_schedule(self, sched_id):
        cfg = self._read()
        cfg['schedules'] = [s for s in cfg['schedules'] if s.get('id') != sched_id]
        self._write(cfg)

    # History
    def get_history(self):
        return self._read().get('history', [])

    def add_history(self, item):
        cfg = self._read()
        cfg['history'].insert(0, item)
        max_h = cfg.get('settings', {}).get('max_history', 100)
        cfg['history'] = cfg['history'][:max_h]
        self._write(cfg)

    def delete_history(self, dump_id):
        cfg = self._read()
        cfg['history'] = [h for h in cfg['history'] if h.get('dump_id') != dump_id]
        self._write(cfg)

    # Settings
    def get_settings(self):
        return self._read().get('settings', DEFAULT_CONFIG['settings'])

    def save_settings(self, settings):
        cfg = self._read()
        cfg['settings'] = settings
        self._write(cfg)
