import eventlet
eventlet.monkey_patch()

import io
import os
import json
import time
import uuid
import shutil
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

from flask import Flask, render_template, request, jsonify, send_file, abort
from flask_socketio import SocketIO
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from db_dumper import DatabaseDumper
from config_manager import ConfigManager
from crypto_manager import get_crypto
from notifier import NotificationManager
from retention import RetentionManager
from verifier import DumpVerifier
from compression import CompressionManager
from security import get_audit_logger, get_rbac_manager
from s3_integration import S3Integration
from webdav_integration import WebDAVIntegration
from restorer import RestoreManager, restore_progress, restore_cancel, preview_dump
from backup_tester import BackupTester, get_test_results, get_test_result
import reporter as reporter_module

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.urandom(24)
socketio = SocketIO(app, async_mode='eventlet', cors_allowed_origins='*')

config_manager = ConfigManager('config.json')
crypto         = get_crypto()
retention_mgr  = RetentionManager(config_manager)
verifier       = DumpVerifier()
compressor     = CompressionManager()
audit_logger   = get_audit_logger()
rbac_manager   = get_rbac_manager()


def _notifier_fn(event: str, details: dict):
    """Notification bridge for BackupTester."""
    settings = config_manager.get_settings()
    notifier = NotificationManager(settings)
    notifier.notify(event, details)


restore_manager = RestoreManager(
    progress_callback=lambda rid, data: socketio.emit(
        'restore_progress', {'restore_id': rid, **data}, namespace='/'
    )
)
backup_tester   = BackupTester(restore_manager=restore_manager,
                                notifier_fn=_notifier_fn)

scheduler = BackgroundScheduler()
scheduler.start()

active_jobs    = {}   # sched_id -> APScheduler job
dump_progress  = {}   # dump_id  -> progress dict
cancel_flags   = {}   # dump_id  -> bool (True = cancel requested)

# ── Health check cache ────────────────────────────────────────────────────────
_health_cache     = {}   # db_id -> result dict
_health_cache_ts  = {}   # db_id -> float (epoch seconds)
_HEALTH_CACHE_TTL = 30   # seconds


# ── Progress helpers ──────────────────────────────────────────────────────────

def emit_progress(dump_id, data):
    dump_progress[dump_id] = data
    socketio.emit('progress', {'dump_id': dump_id, **data}, namespace='/')


def is_cancelled(dump_id: str) -> bool:
    return cancel_flags.get(dump_id, False)


# ── Core dump task ────────────────────────────────────────────────────────────

def _run_dump_task(db_config, dump_id, save_path):
    """Runs inside a socketio background task (eventlet greenlet)."""
    cancel_flags[dump_id] = False

    try:
        db_name = db_config.get('database', 'dump')
        emit_progress(dump_id, {
            'status':     'running',
            'percent':    0,
            'message':    'Initializing dump…',
            'db_name':    db_name,
            'started_at': datetime.now().isoformat(),
        })

        # Decrypt passwords before use
        db_config = crypto.decrypt_db_config(db_config)

        # Local disk space check
        try:
            os.makedirs(save_path, exist_ok=True)
            required_mb = db_config.get('estimated_size_mb', 500)
            free_mb = shutil.disk_usage(save_path).free / (1024 * 1024)
            if free_mb < required_mb * 1.2:
                raise RuntimeError(
                    f'Not enough local disk space. '
                    f'Required: ~{required_mb} MB, Available: {free_mb:.0f} MB'
                )
        except RuntimeError as e:
            emit_progress(dump_id, {'status': 'error', 'percent': 0, 'message': str(e)})
            return

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Store dumps in a per-DB subdirectory: save_path/<db_name>/
        safe_db_name = ''.join(c if c.isalnum() or c in '-_.' else '_' for c in db_name)
        db_save_path = os.path.join(save_path, safe_db_name)
        os.makedirs(db_save_path, exist_ok=True)

        filename  = f"{safe_db_name}_{timestamp}.sql"
        filepath  = os.path.join(db_save_path, filename)

        emit_progress(dump_id, {
            'status':     'running',
            'percent':    2,
            'message':    f'Saving to: {db_save_path}',
            'db_name':    db_name,
        })

        dumper = DatabaseDumper(db_config, socketio, dump_id, emit_progress,
                                cancel_check=lambda: is_cancelled(dump_id))

        dump_start_time = time.time()
        success = dumper.dump(filepath)
        dump_duration_s = time.time() - dump_start_time

        # Cancelled?
        if cancel_flags.get(dump_id):
            emit_progress(dump_id, {
                'status':      'cancelled',
                'percent':     0,
                'message':     'Dump cancelled by user',
                'finished_at': datetime.now().isoformat(),
            })
            # Clean up partial file
            fp = getattr(dumper, '_local_filepath_actual', None) or filepath
            if fp and os.path.exists(fp):
                try:
                    if os.path.isdir(fp):
                        shutil.rmtree(fp)
                    else:
                        os.remove(fp)
                except Exception:
                    pass
            cancel_flags.pop(dump_id, None)
            return

        settings = config_manager.get_settings()
        notifier = NotificationManager(settings)

        if success:
            actual_filepath = getattr(dumper, '_local_filepath_actual', None) or filepath
            actual_filename = os.path.basename(actual_filepath)
            uncompressed_size = os.path.getsize(actual_filepath) if os.path.exists(actual_filepath) else 0
            size = uncompressed_size

            # ── Compression on-the-fly ────────────────────────────────────────
            comp_cfg = settings.get('compression', {})
            comp_fmt = comp_cfg.get('format', 'none')
            compressed_size = None
            if comp_fmt and comp_fmt != 'none' and not os.path.isdir(actual_filepath):
                try:
                    emit_progress(dump_id, {
                        'status': 'running', 'percent': 100,
                        'message': f'Compressing ({comp_fmt})…'
                    })
                    compressed_path = compressor.compress_file(
                        actual_filepath, fmt=comp_fmt,
                        level=comp_cfg.get('level'),
                        remove_src=True
                    )
                    actual_filepath = compressed_path
                    actual_filename = os.path.basename(actual_filepath)
                    compressed_size = os.path.getsize(actual_filepath) if os.path.exists(actual_filepath) else 0
                    size = compressed_size
                    logger.info(f'Dump compressed: {actual_filename}')
                except Exception as e:
                    logger.warning(f'Compression failed (continuing without): {e}')

            # ── Dump statistics ───────────────────────────────────────────────
            # Use a minimum 1 ms threshold to avoid unrealistic speed values
            _eff_duration = max(dump_duration_s, 0.001)
            speed_mbps = round(uncompressed_size / _eff_duration / 1048576, 3) if uncompressed_size > 0 else 0.0
            compression_ratio = round(uncompressed_size / compressed_size, 2) if compressed_size and compressed_size > 0 else None
            estimated_restore_time_s = round(size / (speed_mbps * 1048576), 1) if speed_mbps > 0 else None
            rows_exported   = getattr(dumper, 'rows_exported', 0)
            tables_exported = getattr(dumper, 'tables_exported', 0)

            # Verification
            verify_result = None
            if settings.get('auto_verify', False):
                emit_progress(dump_id, {
                    'status': 'running', 'percent': 100,
                    'message': 'Verifying dump integrity…'
                })
                verify_result = verifier.verify(actual_filepath,
                                                db_type=db_config.get('type', 'postgresql'))

            # ── Cloud upload ──────────────────────────────────────────────────
            cloud_url = None
            storage_cfg = settings.get('storage', {})

            s3_cfg = storage_cfg.get('s3', {})
            if s3_cfg.get('enabled') and os.path.isfile(actual_filepath):
                try:
                    emit_progress(dump_id, {'status': 'running', 'percent': 100, 'message': 'Uploading to S3…'})
                    s3 = S3Integration(s3_cfg)
                    res = s3.upload_file(actual_filepath)
                    if res['ok']:
                        cloud_url = res.get('url', '')
                        s3.apply_retention()
                        audit_logger.log('s3_upload', resource=actual_filename,
                                         status='ok', details=res.get('key'))
                    else:
                        logger.warning(f'S3 upload failed: {res["message"]}')
                        audit_logger.log('s3_upload', resource=actual_filename,
                                         status='error', details=res['message'])
                except Exception as e:
                    logger.warning(f'S3 upload exception: {e}')

            webdav_cfg = storage_cfg.get('webdav', {})
            if webdav_cfg.get('enabled') and os.path.isfile(actual_filepath):
                try:
                    emit_progress(dump_id, {'status': 'running', 'percent': 100, 'message': 'Uploading to WebDAV…'})
                    wdav = WebDAVIntegration(webdav_cfg)
                    res = wdav.upload_file(actual_filepath)
                    if res['ok']:
                        audit_logger.log('webdav_upload', resource=actual_filename, status='ok')
                    else:
                        logger.warning(f'WebDAV upload failed: {res["message"]}')
                except Exception as e:
                    logger.warning(f'WebDAV upload exception: {e}')

            history_item = {
                'dump_id':                dump_id,
                'db_id':                  db_config.get('id'),
                'db_name':                db_name,
                'filename':               actual_filename,
                'filepath':               actual_filepath,
                'size':                   size,
                'uncompressed_size':      uncompressed_size,
                'compressed_size':        compressed_size,
                'compression_method':     comp_fmt if (comp_fmt and comp_fmt != 'none') else None,
                'duration_s':             round(dump_duration_s, 2),
                'speed_mbps':             speed_mbps,
                'compression_ratio':      compression_ratio,
                'rows_exported':          rows_exported,
                'tables_exported':        tables_exported,
                'estimated_restore_time_s': estimated_restore_time_s,
                'status':                 'done',
                'created_at':             datetime.now().isoformat(),
                'verify':                 verify_result,
                'cloud_url':              cloud_url,
            }
            config_manager.add_history(history_item)

            finished_at = datetime.now().isoformat()
            emit_progress(dump_id, {
                'status':      'done',
                'percent':     100,
                'message':     f'Completed: {actual_filename}',
                'file':        actual_filepath,
                'filename':    actual_filename,
                'size':        size,
                'duration_s':  round(dump_duration_s, 2),
                'speed_mbps':  speed_mbps,
                'finished_at': finished_at,
                'verify':      verify_result,
                'cloud_url':   cloud_url,
            })

            # Browser notification event
            socketio.emit('dump_notification', {
                'dump_id':     dump_id,
                'status':      'done',
                'db_name':     db_name,
                'filename':    actual_filename,
                'size':        size,
                'duration_s':  round(dump_duration_s, 2),
                'speed_mbps':  speed_mbps,
                'finished_at': finished_at,
                'cloud_url':   cloud_url,
            }, namespace='/')

            # Notifications
            notifier.notify('success', {
                'db_name':     db_name,
                'filename':    actual_filename,
                'size':        size,
                'finished_at': finished_at,
                'cloud_url':   cloud_url,
            })

            # Audit log
            audit_logger.log('dump_success', resource=actual_filename,
                             details=f'size={size}, db={db_name}')

            # Retention policy
            retention_mgr.apply(db_config.get('id'))

        else:
            last_msg = dump_progress.get(dump_id, {}).get('message', 'Unknown error')
            finished_at = datetime.now().isoformat()
            emit_progress(dump_id, {
                'status':      'error',
                'percent':     0,
                'message':     last_msg,
                'finished_at': finished_at,
            })
            # Browser notification event for errors
            socketio.emit('dump_notification', {
                'dump_id':     dump_id,
                'status':      'error',
                'db_name':     db_name,
                'message':     last_msg,
                'finished_at': finished_at,
            }, namespace='/')
            notifier.notify('error', {
                'db_name':     db_name,
                'message':     last_msg,
                'finished_at': finished_at,
            })
            audit_logger.log('dump_error', resource=db_name, status='error', details=last_msg)

    except Exception as e:
        logger.exception('_run_dump_task')
        emit_progress(dump_id, {
            'status':      'error',
            'percent':     0,
            'message':     str(e),
            'finished_at': datetime.now().isoformat(),
        })
    finally:
        cancel_flags.pop(dump_id, None)


def run_dump(db_config, dump_id, save_path):
    socketio.start_background_task(_run_dump_task, db_config, dump_id, save_path)


def _run_scheduled_dump_with_retry(db_config, save_path, max_retries: int = 3,
                                   base_wait: float = 60.0):
    """
    Scheduled dump with exponential-backoff retry.
    Retries up to max_retries times on failure.
    Wait times: base_wait, 2*base_wait, 4*base_wait, ...
    """
    import eventlet
    last_error = None
    for attempt in range(max_retries + 1):
        dump_id = str(uuid.uuid4())
        cancel_flags[dump_id] = False
        db_name = db_config.get('database', '?')

        if attempt > 0:
            wait = base_wait * (2 ** (attempt - 1))
            logger.info(f'Retry {attempt}/{max_retries} for {db_name} — waiting {wait}s')
            eventlet.sleep(wait)

        _run_dump_task(db_config, dump_id, save_path)

        final = dump_progress.get(dump_id, {})
        if final.get('status') == 'done':
            return  # success
        last_error = final.get('message', 'Unknown error')
        logger.warning(f'Scheduled dump attempt {attempt+1} failed: {last_error}')

    # All retries exhausted
    settings = config_manager.get_settings()
    notifier = NotificationManager(settings)
    notifier.notify('error', {
        'db_name':     db_name,
        'message':     f'All {max_retries+1} attempts failed. Last error: {last_error}',
        'finished_at': datetime.now().isoformat(),
    })
    audit_logger.log('dump_retry_exhausted', resource=db_name, status='error',
                     details=f'retries={max_retries}, last_error={last_error}')


# ════════════════════════════════════════════════════════════════════════════
#  Routes
# ════════════════════════════════════════════════════════════════════════════

@app.route('/')
def index():
    return render_template('index.html')


# ── Databases ─────────────────────────────────────────────────────────────────

@app.route('/api/databases', methods=['GET'])
def get_databases():
    dbs = config_manager.get_databases()
    # Never send encrypted passwords to the UI — mask them
    safe = []
    for db in dbs:
        d = dict(db)
        for field in ('password', 'ssh_password'):
            if d.get(field):
                d[field] = '••••••••'
        safe.append(d)
    return jsonify(safe)


@app.route('/api/databases/raw/<db_id>', methods=['GET'])
def get_database_raw(db_id):
    """Used by edit form — returns decrypted config (server-side only)."""
    db = config_manager.get_database(db_id)
    if not db:
        abort(404)
    decrypted = crypto.decrypt_db_config(db)
    return jsonify(decrypted)


@app.route('/api/databases', methods=['POST'])
def add_database():
    data = request.json
    data['id'] = str(uuid.uuid4())
    encrypted = crypto.encrypt_db_config(data)
    config_manager.add_database(encrypted)
    return jsonify({'ok': True, 'id': data['id']})


@app.route('/api/databases/<db_id>', methods=['PUT'])
def update_database(db_id):
    data = request.json
    data['id'] = db_id
    # Only re-encrypt if password was actually changed (not the mask)
    existing = config_manager.get_database(db_id) or {}
    for field in ('password', 'ssh_password'):
        if data.get(field) == '••••••••':
            data[field] = existing.get(field, '')  # keep existing encrypted
    encrypted = crypto.encrypt_db_config(data)
    config_manager.update_database(db_id, encrypted)
    return jsonify({'ok': True})


@app.route('/api/databases/<db_id>', methods=['DELETE'])
def delete_database(db_id):
    config_manager.delete_database(db_id)
    return jsonify({'ok': True})


@app.route('/api/databases/<db_id>/test', methods=['POST'])
def test_connection(db_id):
    body = request.json or {}
    if body:
        db = body
        db.setdefault('id', db_id)
        # If passwords are masked, load real ones from stored config
        existing = config_manager.get_database(db_id) or {}
        for field in ('password', 'ssh_password'):
            if db.get(field) == '••••••••':
                db[field] = crypto.decrypt(existing.get(field, ''))
    else:
        stored = config_manager.get_database(db_id)
        if not stored:
            return jsonify({'ok': False, 'message': 'Database not found'}), 404
        db = crypto.decrypt_db_config(stored)

    dumper = DatabaseDumper(db, socketio, None, None)
    ok, msg = dumper.test_connection()
    return jsonify({'ok': ok, 'message': msg})


# ── Dumps ─────────────────────────────────────────────────────────────────────

@app.route('/api/dump/start', methods=['POST'])
def start_dump():
    data  = request.json
    db_id = data.get('db_id')
    db    = config_manager.get_database(db_id)
    if not db:
        return jsonify({'ok': False, 'message': 'Database not found'}), 404

    save_path = (data.get('save_path') or
                 config_manager.get_settings().get('default_save_path', './dumps'))
    dump_id = str(uuid.uuid4())
    run_dump(db, dump_id, save_path)
    return jsonify({'ok': True, 'dump_id': dump_id})


@app.route('/api/dump/cancel/<dump_id>', methods=['POST'])
def cancel_dump(dump_id):
    if dump_id in dump_progress:
        cancel_flags[dump_id] = True
        return jsonify({'ok': True, 'message': 'Cancellation requested'})
    return jsonify({'ok': False, 'message': 'Dump not found'}), 404


@app.route('/api/dump/progress', methods=['GET'])
def get_all_progress():
    return jsonify(dump_progress)


@app.route('/api/dump/download/<dump_id>')
def download_dump(dump_id):
    info = dump_progress.get(dump_id)
    if not info or info.get('status') != 'done':
        abort(404)
    filepath = info.get('file')
    if not filepath or not os.path.exists(filepath):
        abort(404)
    return send_file(filepath, as_attachment=True, download_name=info.get('filename'))


# ── History ───────────────────────────────────────────────────────────────────

@app.route('/api/history')
def get_history():
    return jsonify(config_manager.get_history())


@app.route('/api/history/download/<filename>')
def download_history_file(filename):
    for item in config_manager.get_history():
        if item.get('filename') == filename:
            fp = item.get('filepath')
            if fp and os.path.exists(fp):
                return send_file(fp, as_attachment=True, download_name=filename)
    abort(404)


@app.route('/api/history/item/<dump_id>', methods=['DELETE'])
def delete_history_item(dump_id):
    config_manager.delete_history(dump_id)
    return jsonify({'ok': True})


@app.route('/api/history/item/<dump_id>/verify', methods=['POST'])
def verify_dump(dump_id):
    for item in config_manager.get_history():
        if item.get('dump_id') == dump_id:
            fp = item.get('filepath')
            if not fp or not os.path.exists(fp):
                return jsonify({'ok': False, 'message': 'File not found on disk'})
            db = config_manager.get_database(item.get('db_id', '')) or {}
            result = verifier.verify(fp, db_type=db.get('type', 'postgresql'))
            # Update history with verify result
            history = config_manager.get_history()
            for h in history:
                if h.get('dump_id') == dump_id:
                    h['verify'] = result
            config_manager._write({'databases': config_manager.get_databases(),
                                   'schedules': config_manager.get_schedules(),
                                   'history':   history,
                                   'settings':  config_manager.get_settings()})
            return jsonify({'ok': True, 'result': result})
    return jsonify({'ok': False, 'message': 'Not found'}), 404


# ── Settings ──────────────────────────────────────────────────────────────────

@app.route('/api/settings', methods=['GET'])
def get_settings():
    s = config_manager.get_settings()
    # Mask notification passwords in response
    safe = json.loads(json.dumps(s))
    notif = safe.get('notifications', {})
    email = notif.get('email', {})
    if email.get('smtp_password'):
        email['smtp_password'] = '••••••••'
    tg = notif.get('telegram', {})
    if tg.get('bot_token'):
        tg['bot_token'] = tg['bot_token'][:8] + '…'
    return jsonify(safe)


@app.route('/api/settings', methods=['POST'])
def save_settings_route():
    new_settings = request.json

    # Preserve existing encrypted notification secrets if masked
    existing = config_manager.get_settings()
    notif_existing = existing.get('notifications', {})
    notif_new      = new_settings.get('notifications', {})

    email_ex = notif_existing.get('email', {})
    email_new = notif_new.get('email', {})
    if email_new.get('smtp_password') == '••••••••':
        email_new['smtp_password'] = email_ex.get('smtp_password', '')

    tg_ex  = notif_existing.get('telegram', {})
    tg_new = notif_new.get('telegram', {})
    if tg_new.get('bot_token', '').endswith('…'):
        tg_new['bot_token'] = tg_ex.get('bot_token', '')

    config_manager.save_settings(new_settings)
    return jsonify({'ok': True})


@app.route('/api/notifications/test/<channel>', methods=['POST'])
def test_notification(channel):
    settings = config_manager.get_settings()
    notifier = NotificationManager(settings)
    ok, msg  = notifier.test(channel)
    return jsonify({'ok': ok, 'message': msg})


# ── Retention ────────────────────────────────────────────────────────────────

@app.route('/api/retention/preview', methods=['GET'])
def retention_preview():
    would_delete = retention_mgr.preview()
    return jsonify(would_delete)


@app.route('/api/retention/apply', methods=['POST'])
def retention_apply():
    deleted = retention_mgr.apply()
    return jsonify({'ok': True, 'deleted': deleted, 'count': len(deleted)})


# ── Schedules ────────────────────────────────────────────────────────────────

@app.route('/api/schedules', methods=['GET'])
def get_schedules():
    return jsonify(config_manager.get_schedules())


@app.route('/api/schedules', methods=['POST'])
def add_schedule():
    data     = request.json
    sched_id = str(uuid.uuid4())
    data['id']      = sched_id
    data['enabled'] = True

    db        = config_manager.get_database(data['db_id'])
    save_path = (data.get('save_path') or
                 config_manager.get_settings().get('default_save_path', './dumps'))
    max_retries = int(data.get('max_retries', 0))
    base_wait   = float(data.get('retry_wait', 60))

    def scheduled_dump():
        if max_retries > 0:
            socketio.start_background_task(
                _run_scheduled_dump_with_retry, db, save_path, max_retries, base_wait
            )
        else:
            dump_id = str(uuid.uuid4())
            socketio.start_background_task(_run_dump_task, db, dump_id, save_path)

    job = scheduler.add_job(scheduled_dump, CronTrigger.from_crontab(data['cron']),
                            id=sched_id, replace_existing=True)
    active_jobs[sched_id] = job
    config_manager.add_schedule(data)
    audit_logger.log('schedule_created', resource=sched_id,
                     details=f'cron={data["cron"]}, db={db.get("name") if db else "?"}')
    return jsonify({'ok': True, 'id': sched_id})


@app.route('/api/schedules/<sched_id>', methods=['DELETE'])
def delete_schedule(sched_id):
    try:
        scheduler.remove_job(sched_id)
    except Exception:
        pass
    active_jobs.pop(sched_id, None)
    config_manager.delete_schedule(sched_id)
    return jsonify({'ok': True})


@app.route('/api/schedules/<sched_id>/toggle', methods=['POST'])
def toggle_schedule(sched_id):
    schedules = config_manager.get_schedules()
    for s in schedules:
        if s['id'] == sched_id:
            s['enabled'] = not s.get('enabled', True)
            try:
                if s['enabled']:
                    scheduler.resume_job(sched_id)
                else:
                    scheduler.pause_job(sched_id)
            except Exception:
                pass
            config_manager.save_schedules(schedules)
            return jsonify({'ok': True, 'enabled': s['enabled']})
    return jsonify({'ok': False}), 404


# ── Stats ─────────────────────────────────────────────────────────────────────

@app.route('/api/stats')
def get_stats():
    history = config_manager.get_history()

    # Size over time (last 30 dumps)
    timeline = []
    for h in reversed(history[:30]):
        timeline.append({
            'date':    h.get('created_at', '')[:10],
            'size_mb': round(h.get('size', 0) / 1_048_576, 2),
            'db_name': h.get('db_name', ''),
            'status':  h.get('status', ''),
        })

    # Success rate last 30 days
    total  = len(history)
    done   = sum(1 for h in history if h.get('status') == 'done')
    errors = sum(1 for h in history if h.get('status') == 'error')

    # Size by database — with min, max, last, count
    by_db: dict[str, dict] = {}
    for h in history:
        if h.get('status') != 'done':
            continue
        name = h.get('db_name', '?')
        sz   = h.get('size', 0)
        if name not in by_db:
            by_db[name] = {'count': 0, 'total_size': 0, 'last_size': 0,
                           'min_size': sz, 'max_size': sz, 'sizes': []}
        by_db[name]['count']      += 1
        by_db[name]['total_size'] += sz
        by_db[name]['last_size']   = sz   # history is newest-first
        by_db[name]['min_size']    = min(by_db[name]['min_size'], sz)
        by_db[name]['max_size']    = max(by_db[name]['max_size'], sz)
        by_db[name]['sizes'].append(sz)

    top_dbs = sorted(by_db.items(), key=lambda x: x[1]['last_size'], reverse=True)[:8]

    def _strip(d):
        d.pop('sizes', None)
        return d

    return jsonify({
        'total':        total,
        'done':         done,
        'errors':       errors,
        'success_rate': round(done / total * 100, 1) if total else 0,
        'timeline':     timeline,
        'by_db':        [{'name': k, **_strip(v)} for k, v in top_dbs],
    })


# ── Disk ─────────────────────────────────────────────────────────────────────

@app.route('/api/disk', methods=['POST'])
def check_disk():
    path = request.json.get('path', '.')
    try:
        usage = shutil.disk_usage(path)
        return jsonify({
            'ok':       True,
            'total_gb': round(usage.total / 1e9, 2),
            'used_gb':  round(usage.used  / 1e9, 2),
            'free_gb':  round(usage.free  / 1e9, 2),
            'percent':  round(usage.used  / usage.total * 100, 1),
        })
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)})


# ── Startup ───────────────────────────────────────────────────────────────────

def restore_schedules():
    for s in config_manager.get_schedules():
        if not s.get('enabled', True):
            continue
        db = config_manager.get_database(s['db_id'])
        if not db:
            continue
        save_path   = (s.get('save_path') or
                       config_manager.get_settings().get('default_save_path', './dumps'))
        max_retries = int(s.get('max_retries', 0))
        base_wait   = float(s.get('retry_wait', 60))

        def make_job(db_cfg, sp, mr, bw):
            def fn():
                if mr > 0:
                    socketio.start_background_task(
                        _run_scheduled_dump_with_retry, db_cfg, sp, mr, bw
                    )
                else:
                    socketio.start_background_task(
                        _run_dump_task, db_cfg, str(uuid.uuid4()), sp
                    )
            return fn

        try:
            job = scheduler.add_job(make_job(db, save_path, max_retries, base_wait),
                                    CronTrigger.from_crontab(s['cron']),
                                    id=s['id'], replace_existing=True)
            active_jobs[s['id']] = job
        except Exception as e:
            logger.warning(f'Could not restore schedule {s["id"]}: {e}')


# ── Audit Log ─────────────────────────────────────────────────────────────────

@app.route('/api/audit', methods=['GET'])
def get_audit_logs():
    limit  = int(request.args.get('limit', 100))
    offset = int(request.args.get('offset', 0))
    user   = request.args.get('user') or None
    action = request.args.get('action') or None
    since  = request.args.get('since') or None
    until  = request.args.get('until') or None
    logs   = audit_logger.get_logs(limit=limit, offset=offset,
                                   user=user, action=action, since=since, until=until)
    total  = audit_logger.get_total(user=user, action=action, since=since, until=until)
    return jsonify({'logs': logs, 'total': total})


@app.route('/api/audit/purge', methods=['POST'])
def purge_audit_logs():
    keep_days = int((request.json or {}).get('keep_days', 90))
    deleted   = audit_logger.purge_old(keep_days=keep_days)
    return jsonify({'ok': True, 'deleted': deleted})


# ── Storage (S3 / WebDAV) ─────────────────────────────────────────────────────

@app.route('/api/storage/s3/test', methods=['POST'])
def test_s3():
    settings = config_manager.get_settings()
    s3_cfg = settings.get('storage', {}).get('s3', {})
    s3 = S3Integration(s3_cfg)
    ok, msg = s3.test_connection()
    return jsonify({'ok': ok, 'message': msg})


@app.route('/api/storage/s3/list', methods=['GET'])
def list_s3():
    settings = config_manager.get_settings()
    s3_cfg = settings.get('storage', {}).get('s3', {}  )
    s3 = S3Integration(s3_cfg)
    objects = s3.list_objects()
    return jsonify(objects)


@app.route('/api/storage/s3/delete', methods=['POST'])
def delete_s3_object():
    key = (request.json or {}).get('key', '')
    if not key:
        return jsonify({'ok': False, 'message': 'key required'}), 400
    settings = config_manager.get_settings()
    s3_cfg = settings.get('storage', {}).get('s3', {})
    s3 = S3Integration(s3_cfg)
    ok = s3.delete_object(key)
    return jsonify({'ok': ok})


@app.route('/api/storage/webdav/test', methods=['POST'])
def test_webdav():
    settings = config_manager.get_settings()
    wdav_cfg = settings.get('storage', {}).get('webdav', {})
    wdav = WebDAVIntegration(wdav_cfg)
    ok, msg = wdav.test_connection()
    return jsonify({'ok': ok, 'message': msg})


@app.route('/api/storage/webdav/list', methods=['GET'])
def list_webdav():
    settings = config_manager.get_settings()
    wdav_cfg = settings.get('storage', {}).get('webdav', {})
    wdav = WebDAVIntegration(wdav_cfg)
    files = wdav.list_files()
    return jsonify(files)


# ── Export / Import database configurations ───────────────────────────────────

@app.route('/api/databases/export', methods=['GET'])
def export_databases():
    dbs = config_manager.get_databases()
    decrypted = [crypto.decrypt_db_config(db) for db in dbs]
    return jsonify(decrypted)


@app.route('/api/databases/import', methods=['POST'])
def import_databases():
    incoming = request.json
    if not isinstance(incoming, list):
        return jsonify({'ok': False, 'message': 'Expected a JSON array'}), 400
    added = 0
    for db in incoming:
        if not isinstance(db, dict):
            continue
        db['id'] = str(uuid.uuid4())
        encrypted = crypto.encrypt_db_config(db)
        config_manager.add_database(encrypted)
        added += 1
    audit_logger.log('databases_imported', details=f'count={added}')
    return jsonify({'ok': True, 'added': added})


# ── Compression formats ───────────────────────────────────────────────────────

@app.route('/api/compression/formats', methods=['GET'])
def list_compression_formats():
    from compression import SUPPORTED_FORMATS, EXTENSIONS
    formats = [{'id': fmt, 'ext': EXTENSIONS[fmt]} for fmt in SUPPORTED_FORMATS]
    formats.insert(0, {'id': 'none', 'ext': ''})
    return jsonify(formats)


# ── Restore ───────────────────────────────────────────────────────────────────

@app.route('/api/restore/preview/<dump_id>', methods=['POST'])
def restore_preview(dump_id):
    """Return table list, row counts, file size for a history dump."""
    for item in config_manager.get_history():
        if item.get('dump_id') == dump_id:
            fp = item.get('filepath')
            if not fp or not os.path.exists(fp):
                return jsonify({'ok': False, 'message': 'File not found on disk'}), 404
            try:
                preview = preview_dump(fp)
                return jsonify({'ok': True, **preview})
            except Exception as e:
                return jsonify({'ok': False, 'message': str(e)}), 500
    return jsonify({'ok': False, 'message': 'History item not found'}), 404


@app.route('/api/restore/start', methods=['POST'])
def start_restore():
    """
    Start a restore operation.
    Body: {dump_id, db_id, tables: [...], threads: int}
    """
    data    = request.json or {}
    dump_id = data.get('dump_id')
    db_id   = data.get('db_id')
    tables  = data.get('tables') or None
    threads = int(data.get('threads', 1))

    # Find dump file
    dump_file = None
    for item in config_manager.get_history():
        if item.get('dump_id') == dump_id:
            dump_file = item.get('filepath')
            break
    if not dump_file:
        return jsonify({'ok': False, 'message': 'Dump not found in history'}), 404
    if not os.path.exists(dump_file):
        return jsonify({'ok': False, 'message': 'Dump file missing from disk'}), 404

    # Load target database config
    db = config_manager.get_database(db_id)
    if not db:
        return jsonify({'ok': False, 'message': 'Target database not found'}), 404
    db = crypto.decrypt_db_config(db)

    restore_id = restore_manager.start(db, dump_file, tables=tables, threads=threads)
    audit_logger.log('restore_started', resource=dump_file,
                     details=f'restore_id={restore_id}, db={db.get("database")}')
    return jsonify({'ok': True, 'restore_id': restore_id})


@app.route('/api/restore/progress/<restore_id>', methods=['GET'])
def get_restore_progress(restore_id):
    prog = restore_progress.get(restore_id)
    if prog is None:
        return jsonify({'ok': False, 'message': 'Restore not found'}), 404
    return jsonify({'ok': True, **prog})


@app.route('/api/restore/cancel/<restore_id>', methods=['POST'])
def cancel_restore(restore_id):
    ok = restore_manager.cancel(restore_id)
    if ok:
        return jsonify({'ok': True, 'message': 'Cancellation requested'})
    return jsonify({'ok': False, 'message': 'Restore not found'}), 404


@app.route('/api/restore/all', methods=['GET'])
def get_all_restores():
    return jsonify(restore_progress)


# ── Backup testing ────────────────────────────────────────────────────────────

@app.route('/api/test-backup/run', methods=['POST'])
def run_backup_test():
    """
    Schedule an automated restore test.
    Body: {dump_id, db_id, cleanup: bool}
    """
    data    = request.json or {}
    dump_id = data.get('dump_id', '')
    db_id   = data.get('db_id', '')
    cleanup = data.get('cleanup', True)

    # Find dump file
    dump_file = None
    for item in config_manager.get_history():
        if item.get('dump_id') == dump_id:
            dump_file = item.get('filepath')
            break
    if not dump_file:
        return jsonify({'ok': False, 'message': 'Dump not found'}), 404

    # Load test database config
    db = config_manager.get_database(db_id)
    if not db:
        return jsonify({'ok': False, 'message': 'Test database not found'}), 404
    db = crypto.decrypt_db_config(db)

    test_id = backup_tester.run_test(
        dump_file=dump_file,
        test_db_config=db,
        dump_id=dump_id,
        cleanup=cleanup,
    )
    audit_logger.log('backup_test_started', resource=dump_file,
                     details=f'test_id={test_id}')
    return jsonify({'ok': True, 'test_id': test_id})


@app.route('/api/test-backup/results', methods=['GET'])
def get_backup_test_results():
    limit  = int(request.args.get('limit', 50))
    offset = int(request.args.get('offset', 0))
    return jsonify(get_test_results(limit=limit, offset=offset))


@app.route('/api/test-backup/results/<test_id>', methods=['GET'])
def get_backup_test_result(test_id):
    result = get_test_result(test_id)
    if not result:
        return jsonify({'ok': False, 'message': 'Test result not found'}), 404
    return jsonify({'ok': True, **result})


# ── Reports & Analytics ───────────────────────────────────────────────────────

@app.route('/api/reports/summary', methods=['GET'])
def reports_summary():
    period = int(request.args.get('days', 30))
    history = config_manager.get_history()
    summary = reporter_module.get_summary(history, period_days=period)
    return jsonify(summary)


@app.route('/api/reports/trends', methods=['GET'])
def reports_trends():
    history   = config_manager.get_history()
    analytics = reporter_module.compute_analytics(history)
    return jsonify({
        'trends':       analytics['trends'],
        'by_db':        analytics['by_db'],
        'success_rate': analytics['success_rate'],
        'sla':          analytics['sla'],
        'top_errors':   analytics['top_errors'],
    })


@app.route('/api/reports/compliance', methods=['GET'])
def reports_compliance():
    history  = config_manager.get_history()
    settings = config_manager.get_settings()
    report   = reporter_module.compliance_report(history, settings)
    return jsonify(report)


@app.route('/api/reports/export/<fmt>', methods=['GET'])
def reports_export(fmt: str):
    history = config_manager.get_history()
    period  = int(request.args.get('days', 30))

    if fmt == 'csv':
        csv_bytes = reporter_module.export_csv(history)
        filename  = f'backup_report_{datetime.now().strftime("%Y%m%d")}.csv'
        return send_file(
            io.BytesIO(csv_bytes),
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename,
        )

    if fmt == 'pdf':
        pdf_bytes = reporter_module.export_pdf(history, period_days=period)
        if pdf_bytes is None:
            return jsonify({
                'ok': False,
                'message': 'reportlab not installed. Run: pip install reportlab'
            }), 422
        filename = f'backup_report_{datetime.now().strftime("%Y%m%d")}.pdf'
        return send_file(
            io.BytesIO(pdf_bytes),
            mimetype='application/pdf',
            as_attachment=True,
            download_name=filename,
        )

    return jsonify({'ok': False, 'message': f'Unsupported format: {fmt}'}), 400


# ── Schedule calendar ─────────────────────────────────────────────────────────

@app.route('/api/schedule/calendar', methods=['GET'])
def schedule_calendar():
    """
    Return upcoming scheduled run times for the next N days.
    Query param: days (default 30)
    """
    try:
        from croniter import croniter
        has_croniter = True
    except ImportError:
        has_croniter = False

    days      = int(request.args.get('days', 30))
    schedules = config_manager.get_schedules()
    now       = datetime.now()
    end       = now + timedelta(days=days)

    calendar_items = []
    for s in schedules:
        if not s.get('enabled', True):
            continue
        cron_expr = s.get('cron', '')
        db_id     = s.get('db_id', '')
        db        = config_manager.get_database(db_id)
        db_name   = db.get('name', db_id) if db else db_id

        if has_croniter:
            try:
                it = croniter(cron_expr, now)
                for _ in range(min(days * 3, 200)):   # cap results
                    nxt = it.get_next(datetime)
                    if nxt > end:
                        break
                    calendar_items.append({
                        'sched_id': s.get('id'),
                        'db_name':  db_name,
                        'cron':     cron_expr,
                        'run_at':   nxt.isoformat(),
                        'enabled':  s.get('enabled', True),
                    })
            except Exception:
                pass
        else:
            # fallback: just return the schedule with next_run from APScheduler
            job = active_jobs.get(s.get('id'))
            next_run = None
            if job:
                try:
                    next_run = job.next_run_time.isoformat() if job.next_run_time else None
                except Exception:
                    pass
            calendar_items.append({
                'sched_id': s.get('id'),
                'db_name':  db_name,
                'cron':     cron_expr,
                'next_run': next_run,
                'enabled':  s.get('enabled', True),
            })

    return jsonify(calendar_items)


# ── Bulk dump operations ──────────────────────────────────────────────────────

@app.route('/api/dumps/bulk-start', methods=['POST'])
def bulk_start_dumps():
    """
    Start dumps for multiple databases.
    Body: {db_ids: [...], save_path: '...'}
    """
    data      = request.json or {}
    db_ids    = data.get('db_ids', [])
    save_path = (data.get('save_path') or
                 config_manager.get_settings().get('default_save_path', './dumps'))

    if not db_ids:
        return jsonify({'ok': False, 'message': 'No db_ids provided'}), 400

    started = []
    failed  = []
    for db_id in db_ids:
        db = config_manager.get_database(db_id)
        if not db:
            failed.append({'db_id': db_id, 'reason': 'not found'})
            continue
        dump_id = str(uuid.uuid4())
        run_dump(db, dump_id, save_path)
        started.append({'db_id': db_id, 'dump_id': dump_id})

    return jsonify({'ok': True, 'started': started, 'failed': failed})


# ── Routes for new pages ──────────────────────────────────────────────────────

@app.route('/restore')
def restore_page():
    return render_template('restore.html')


@app.route('/reports')
def reports_page():
    return render_template('reports.html')


@app.route('/calendar')
def calendar_page():
    return render_template('calendar.html')


@app.route('/health')
def health_page():
    return render_template('health.html')


# ── Service worker (must be served from root scope) ───────────────────────────

@app.route('/service-worker.js')
def service_worker():
    sw_path = os.path.join(app.root_path, 'static', 'service-worker.js')
    if os.path.exists(sw_path):
        return send_file(sw_path, mimetype='application/javascript')
    # Minimal inline service worker if file doesn't exist
    sw_content = (
        "self.addEventListener('push', e => {"
        "  const d = e.data ? e.data.json() : {};"
        "  self.registration.showNotification(d.title || 'DB Dump', {"
        "    body: d.body || '', icon: d.icon || '/static/icon.png', data: d"
        "  });"
        "});"
        "self.addEventListener('notificationclick', e => {"
        "  e.notification.close();"
        "  const url = (e.notification.data && e.notification.data.url) || '/';"
        "  e.waitUntil(clients.openWindow(url));"
        "});"
    )
    from flask import Response
    return Response(sw_content, mimetype='application/javascript')


# ── Health check API ──────────────────────────────────────────────────────────

def _check_db_health(db: dict) -> tuple:
    """Test a single database connection and return (db_id, result_dict)."""
    db_id   = db.get('id', '')
    db_name = db.get('database', db_id)
    now     = time.time()

    # Return cached result if still fresh
    if db_id in _health_cache and now - _health_cache_ts.get(db_id, 0) < _HEALTH_CACHE_TTL:
        return db_id, _health_cache[db_id]

    t0 = time.time()
    try:
        db_decrypted = crypto.decrypt_db_config(db)
        dumper = DatabaseDumper(db_decrypted, None, None, None)
        ok, msg = dumper.test_connection()
    except Exception as exc:
        ok, msg = False, str(exc)

    response_time_ms = round((time.time() - t0) * 1000, 1)
    result = {
        'ok':               ok,
        'message':          msg,
        'name':             db.get('name') or db_name,
        'db_type':          db.get('type', 'postgresql'),
        'checked_at':       datetime.now().isoformat(),
        'response_time_ms': response_time_ms,
    }
    _health_cache[db_id]    = result
    _health_cache_ts[db_id] = time.time()
    return db_id, result


@app.route('/api/health/databases', methods=['GET'])
def health_databases():
    """Return health status for all configured databases (parallel, cached 30 s)."""
    dbs = config_manager.get_databases()
    results = {}
    with ThreadPoolExecutor(max_workers=min(10, max(1, len(dbs)))) as exe:
        futures = [exe.submit(_check_db_health, db) for db in dbs]
        for f in futures:
            try:
                db_id, result = f.result(timeout=15)
                results[db_id] = result
            except Exception as exc:
                logger.warning(f'Health check future failed: {exc}')
    return jsonify(results)


@app.route('/api/health/check/<db_id>', methods=['POST'])
def health_check_single(db_id):
    """Force-refresh health check for a single database (bypass cache)."""
    db = config_manager.get_database(db_id)
    if not db:
        return jsonify({'ok': False, 'message': 'Database not found'}), 404
    # Invalidate cache so _check_db_health performs a fresh check
    _health_cache_ts.pop(db_id, None)
    _, result = _check_db_health(db)
    return jsonify(result)


if __name__ == '__main__':
    restore_schedules()
    print('\n✅  DB Dump Manager running → http://127.0.0.1:5000\n')
    print(f'   Encryption: {"enabled ✓" if crypto.is_available() else "disabled (install cryptography)"}')
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)
    #socketio.run(app, host='127.0.0.1', port=5000, debug=False)
