# DB Dump Manager — API Reference

Base URL: `http://localhost:5000`

All endpoints return JSON. POST/PUT bodies must use `Content-Type: application/json`.

---

## Databases

### `GET /api/databases`
List all configured databases (passwords masked).

**Response:** `[{id, name, type, host, port, user, database, ...}]`

### `POST /api/databases`
Add a new database configuration.

**Body:**
```json
{
  "name": "Production PG",
  "type": "postgresql",
  "host": "192.168.1.10",
  "port": 5432,
  "user": "dbuser",
  "password": "secret",
  "database": "mydb",
  "dump_mode": "full",
  "dump_format": "plain",
  "use_ssh": false
}
```
**Response:** `{"ok": true, "id": "<uuid>"}`

### `GET /api/databases/raw/<db_id>`
Retrieve full decrypted config for editing.

### `PUT /api/databases/<db_id>`
Update an existing database config.

### `DELETE /api/databases/<db_id>`
Delete a database config.

### `POST /api/databases/<db_id>/test`
Test connectivity to the database.

**Response:** `{"ok": true, "message": "Connected successfully"}`

### `GET /api/databases/export`
Export all database configs as decrypted JSON array (for backup).

### `POST /api/databases/import`
Import database configs from a JSON array. New IDs are assigned.

**Body:** `[{...db config...}, ...]`

**Response:** `{"ok": true, "added": 3}`

---

## Dumps

### `POST /api/dump/start`
Start a new dump job.

**Body:** `{"db_id": "<uuid>", "save_path": "./dumps"}`

**Response:** `{"ok": true, "dump_id": "<uuid>"}`

### `POST /api/dump/cancel/<dump_id>`
Request cancellation of a running dump.

### `GET /api/dump/progress`
Get progress for all active dumps.

**Response:** `{"<dump_id>": {status, percent, message, ...}}`

### `GET /api/dump/download/<dump_id>`
Download the completed dump file.

---

## History

### `GET /api/history`
Get dump history (newest first).

### `GET /api/history/download/<filename>`
Download a historical dump by filename.

### `DELETE /api/history/item/<dump_id>`
Remove a history record (does not delete the file).

### `POST /api/history/item/<dump_id>/verify`
Run integrity checks on a dump file.

**Response:**
```json
{
  "ok": true,
  "result": {
    "ok": true,
    "summary": "✅ OK — 3 checks, 12 tables",
    "checks": [{"name": "...", "ok": true, "detail": "..."}]
  }
}
```

---

## Schedules

### `GET /api/schedules`
List all configured schedules.

### `POST /api/schedules`
Create a new schedule.

**Body:**
```json
{
  "db_id": "<uuid>",
  "cron": "0 2 * * *",
  "save_path": "./dumps",
  "max_retries": 3,
  "retry_wait": 60
}
```
- `max_retries`: number of retry attempts on failure (0 = no retries)
- `retry_wait`: base wait time in seconds (doubles each attempt)

### `DELETE /api/schedules/<sched_id>`
Remove a schedule.

### `POST /api/schedules/<sched_id>/toggle`
Enable or disable a schedule.

**Response:** `{"ok": true, "enabled": false}`

---

## Settings

### `GET /api/settings`
Get all application settings (notification passwords masked).

### `POST /api/settings`
Save settings.

**Body (partial):**
```json
{
  "default_save_path": "./dumps",
  "max_history": 100,
  "auto_verify": false,
  "compression": {
    "format": "gzip",
    "level": 6
  },
  "retention": {"enabled": true, "keep_last_n": 10, "keep_days": 30},
  "notifications": {
    "enabled": true,
    "email": {"enabled": true, "smtp_host": "smtp.gmail.com", ...},
    "telegram": {"enabled": false, "bot_token": "...", "chat_id": "..."},
    "webhook": {"enabled": false, "url": "..."}
  },
  "storage": {
    "s3": {
      "enabled": false,
      "bucket": "my-backups",
      "prefix": "db-dumps/",
      "region": "us-east-1",
      "endpoint_url": "",
      "access_key": "",
      "secret_key": "",
      "keep_last_n": 50
    },
    "webdav": {
      "enabled": false,
      "url": "https://cloud.example.com/remote.php/dav/files/user/",
      "username": "",
      "password": "",
      "root_dir": "/db-dumps",
      "keep_last_n": 30
    }
  }
}
```

---

## Notifications

### `POST /api/notifications/test/<channel>`
Send a test notification. Channel: `email`, `telegram`, `webhook`.

---

## Retention

### `GET /api/retention/preview`
Preview what would be deleted by the current retention policy.

### `POST /api/retention/apply`
Apply the retention policy immediately.

---

## Storage

### `POST /api/storage/s3/test`
Test S3/MinIO connection.

### `GET /api/storage/s3/list`
List objects in the configured S3 bucket.

### `POST /api/storage/s3/delete`
Delete an S3 object.

**Body:** `{"key": "db-dumps/mydb_20240101.sql.gz"}`

### `POST /api/storage/webdav/test`
Test WebDAV connection.

### `GET /api/storage/webdav/list`
List files in the configured WebDAV directory.

---

## Audit Log

### `GET /api/audit`
Query audit log entries.

**Query parameters:**
| Param  | Type   | Description                        |
|--------|--------|------------------------------------|
| limit  | int    | Max entries (default 100)          |
| offset | int    | Pagination offset                  |
| user   | string | Filter by username                 |
| action | string | Filter by action (partial match)   |
| since  | string | ISO 8601 timestamp lower bound     |
| until  | string | ISO 8601 timestamp upper bound     |

**Response:**
```json
{
  "logs": [
    {"id": 1, "ts": "2024-01-01T02:00:05", "user": "system",
     "action": "dump_success", "resource": "mydb_20240101.sql.gz",
     "ip": null, "status": "ok", "details": "size=15MB"}
  ],
  "total": 1
}
```

### `POST /api/audit/purge`
Delete old audit entries.

**Body:** `{"keep_days": 90}`

---

## Compression Formats

### `GET /api/compression/formats`
List available compression formats.

**Response:**
```json
[
  {"id": "none", "ext": ""},
  {"id": "gzip", "ext": ".gz"},
  {"id": "bzip2", "ext": ".bz2"},
  {"id": "zstd", "ext": ".zst"}
]
```

---

## System

### `GET /`
Serve the web UI.

### `POST /api/disk`
Check disk usage at a given path.

**Body:** `{"path": "./dumps"}`

**Response:** `{"ok": true, "total_gb": 100.0, "used_gb": 45.3, "free_gb": 54.7, "percent": 45.3}`

---

## WebSocket Events (Socket.IO)

### Server → Client: `progress`
Emitted whenever a dump job status changes.

```json
{
  "dump_id": "<uuid>",
  "status": "running | done | error | cancelled",
  "percent": 75,
  "message": "Dumping table orders…",
  "db_name": "mydb",
  "filename": "mydb_20240101_020000.sql",
  "size": 15728640,
  "started_at": "2024-01-01T02:00:00",
  "finished_at": "2024-01-01T02:01:30",
  "verify": {"ok": true, "summary": "✅ OK — 3 checks"},
  "cloud_url": "https://my-bucket.s3.amazonaws.com/..."
}
```
