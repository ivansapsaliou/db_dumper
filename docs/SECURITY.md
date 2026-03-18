# DB Dump Manager — Security Guide

## Encryption at Rest

### Database Passwords
All database credentials (passwords, SSH passwords) are encrypted using **Fernet symmetric encryption** from the Python `cryptography` library.

- Algorithm: AES-128-CBC with HMAC-SHA256 authentication (Fernet specification)
- Key storage: `.secret.key` file in the application directory
- Key format: URL-safe base64-encoded 32-byte key

**Important:** Never commit `.secret.key` to version control. Add it to `.gitignore`.

### Encryption Key Management
```bash
# Backup your key
cp .secret.key /secure/backup/location/.secret.key

# Rotate key (requires re-encrypting all passwords)
# 1. Export database configs (decrypted)
curl http://localhost:5000/api/databases/export > backup.json

# 2. Delete old key
rm .secret.key

# 3. Restart app (generates new key)
python app.py

# 4. Re-import configs (re-encrypted with new key)
curl -X POST http://localhost:5000/api/databases/import \
  -H 'Content-Type: application/json' \
  -d @backup.json
```

---

## Audit Logging

All security-relevant operations are logged to `audit.db` (SQLite):

| Action | Description |
|--------|-------------|
| `dump_success` | Dump completed successfully |
| `dump_error` | Dump failed |
| `dump_retry_exhausted` | All retries failed |
| `s3_upload` | File uploaded to S3/MinIO |
| `webdav_upload` | File uploaded to WebDAV |
| `schedule_created` | New schedule created |
| `databases_imported` | Database configs imported |

### Viewing Audit Logs
Access via the **Audit Log** page in the web UI or via API:
```bash
curl "http://localhost:5000/api/audit?limit=50&action=dump"
```

### Log Retention
Audit logs are kept for 90 days by default. Configure via API:
```bash
curl -X POST http://localhost:5000/api/audit/purge \
  -H 'Content-Type: application/json' \
  -d '{"keep_days": 90}'
```

---

## Role-Based Access Control (RBAC)

### Roles

| Role     | Databases | Dumps | Schedules | Settings | Audit |
|----------|-----------|-------|-----------|----------|-------|
| admin    | R/W/D     | All   | R/W/D     | R/W      | R     |
| operator | R/W       | All   | R/W       | R        | —     |
| viewer   | R         | DL    | R         | R        | —     |

(R=read, W=write, D=delete, DL=download)

### Default Credentials
On first run, a default admin user is created:
- Username: `admin`
- Password: `admin`

**Change this immediately in production!**

### Managing Users (via API)
```bash
# Create a new operator
curl -X POST http://localhost:5000/api/users \
  -H 'Content-Type: application/json' \
  -d '{"username": "ops1", "password": "strong_pass", "role": "operator"}'

# List users
curl http://localhost:5000/api/users

# Change role
curl -X PATCH http://localhost:5000/api/users/ops1 \
  -H 'Content-Type: application/json' \
  -d '{"role": "viewer"}'
```

---

## Data Masking (PII)

DB Dump Manager supports masking personally identifiable information (PII) in dump files using `DataMasker`.

Supported patterns:
| Pattern | Example | Masked as |
|---------|---------|-----------|
| Email | `user@example.com` | `********` |
| SSN | `123-45-6789` | `********` |
| Credit Card | `4111 1111 1111 1111` | `********` |
| Phone (opt-in) | `+1-555-123-4567` | `********` |

### Usage (programmatic)
```python
from security import DataMasker

masker = DataMasker()
masker.mask_file('dump.sql', 'dump_masked.sql', patterns={'email', 'ssn', 'card'})
```

---

## Network Security

### Recommendations for Production
1. **Run behind a reverse proxy** (nginx/Caddy) with HTTPS
2. **Restrict access** to the application port (5000) using firewall rules
3. **Use SSH key authentication** instead of passwords for SSH tunnels
4. **Rotate S3 access keys** regularly and use IAM roles with minimal permissions
5. **Enable WebDAV over HTTPS** only

### S3/MinIO Security
- Use dedicated IAM users with only `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject` permissions
- Enable bucket versioning for additional protection
- Use server-side encryption (SSE-S3 or SSE-KMS)

### Example Nginx Configuration
```nginx
server {
    listen 443 ssl;
    server_name dumper.example.com;

    ssl_certificate     /etc/ssl/certs/dumper.crt;
    ssl_certificate_key /etc/ssl/private/dumper.key;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_read_timeout 3600;
    }
}
```

---

## Secret Files

The following files contain sensitive data and should **never** be committed to version control:

| File | Content |
|------|---------|
| `.secret.key` | Encryption key for passwords |
| `config.json` | Database configs (encrypted passwords) |
| `audit.db` | Audit log (may contain sensitive operation details) |
| `users.json` | User credentials (hashed passwords) |

Add to `.gitignore`:
```
.secret.key
config.json
audit.db
users.json
dumps/
```
