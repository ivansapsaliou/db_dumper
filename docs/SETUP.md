# DB Dump Manager — Setup Guide

## Quick Start

### Option 1: Docker Compose (recommended)

```bash
# Clone the repository
git clone https://github.com/ivansapsaliou/db_dumper.git
cd db_dumper

# Start all services (app + test databases + MinIO)
docker compose up -d

# Open the web UI
open http://localhost:5000
```

Services:
| Service | URL/Port | Description |
|---------|----------|-------------|
| App     | :5000    | DB Dump Manager web UI |
| PostgreSQL | :5432 | Test database |
| MySQL   | :3306    | Test database |
| MinIO   | :9000 / :9001 | S3-compatible object storage |

---

### Option 2: Local Installation

**Requirements:** Python 3.10+

```bash
cd db_dumper

# Install dependencies
pip install -r requirements.txt

# Start the application
python app.py
```

Open http://127.0.0.1:5000

---

## Configuration

All configuration is stored in `config.json` (created automatically on first run).

The encryption key is stored in `.secret.key` — **keep this file safe and never commit it to version control**.

---

## Adding a Database

1. Click **Databases** in the sidebar
2. Click **+ Add Database**
3. Fill in the connection details
4. Optionally configure SSH tunnel if the DB is behind a bastion host
5. Click **✓ Test** to verify connectivity
6. Click **Save**

### Supported Databases
- PostgreSQL (12+)
- MySQL / MariaDB (8+)
- Oracle (via cx_Oracle)

---

## Starting a Dump

1. On the **Databases** page, click **▶ Dump** next to a database
2. Optionally change the save path
3. Click **▶ Start**
4. Monitor progress on the **Active Dumps** page

---

## Scheduling Automatic Dumps

1. Click **Schedules** → **+ Add Schedule**
2. Select the database
3. Choose a preset frequency (hourly, every 4h, daily, weekly) or enter a custom cron expression
4. Set optional save path
5. Configure retry settings (max retries, wait time between retries)
6. Click **Save**

**Cron expression format:** `minute hour day month weekday`

Examples:
- `0 2 * * *` — every day at 02:00
- `0 */6 * * *` — every 6 hours
- `0 3 * * 1` — every Monday at 03:00

---

## Compression

Configure dump compression in **Settings**:

| Format | Extension | Notes |
|--------|-----------|-------|
| none   | —         | No compression (default) |
| gzip   | .gz       | Fast, widely supported |
| bzip2  | .bz2      | Better ratio, slower |
| zstd   | .zst      | Best speed+ratio (requires `zstandard`) |

---

## Cloud Storage

### Amazon S3 / MinIO

In **Settings → Storage → S3**:
1. Enable S3 storage
2. Enter bucket name, region, access key, secret key
3. For MinIO: set `endpoint_url` to your MinIO server (e.g. `http://localhost:9000`)
4. Click **Test S3 Connection**

Dumps are automatically uploaded after completion.

### WebDAV

In **Settings → Storage → WebDAV**:
1. Enable WebDAV
2. Enter the WebDAV URL (e.g. Nextcloud, ownCloud)
3. Enter username and password
4. Click **Test WebDAV Connection**

---

## Notifications

Configure in **Notifications**:

### Email (SMTP)
- Works with Gmail (use App Password), Outlook, or any SMTP server
- Gmail: enable 2FA and create an App Password at https://myaccount.google.com/apppasswords

### Telegram
1. Create a bot with @BotFather
2. Get your Chat ID from @userinfobot
3. Enter both in the Notifications settings

### Webhook (Slack, etc.)
- POST JSON payload to any URL
- Works with Slack Incoming Webhooks, Discord webhooks, custom endpoints

---

## Security

### Encryption
- All database passwords are encrypted at rest using Fernet symmetric encryption
- The key is stored in `.secret.key` — back it up separately from `config.json`

### Audit Log
- All operations are logged to `audit.db` (SQLite)
- View logs in **Audit Log** page
- Configurable retention (default: 90 days)

### RBAC (Role-Based Access Control)
Default roles:
| Role     | Permissions |
|----------|-------------|
| admin    | Full access including user management |
| operator | Create/manage databases, run dumps, manage schedules |
| viewer   | Read-only access to history and databases |

User management is available via the API (see `docs/API.md`).

---

## Retention Policy

Configure in **Retention**:
- **Keep Last N dumps** per database
- **Delete dumps older than N days**
- Preview and apply retention manually or let it run after each dump

---

## Troubleshooting

### "Not enough disk space" error
- Increase the **Est. DB Size (MB)** field when configuring the database
- Or free up disk space

### PostgreSQL connection fails
- Check that `pg_dump` is installed on the remote server (SSH mode)
- Or use Direct mode if the DB is accessible directly

### Encrypted password issues after moving to new server
- Copy both `config.json` AND `.secret.key` to the new server
- If you lose `.secret.key`, you must re-enter all database passwords

### Dump stuck at 0%
- Check SSH connectivity
- Review application logs: `python app.py` console output
