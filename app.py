import os
import json
import uuid
import shutil
import logging
from datetime import datetime
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.urandom(24)
socketio = SocketIO(app, async_mode='eventlet', cors_allowed_origins='*')

config_manager = ConfigManager('config.json')
crypto         = get_crypto()
retention_mgr  = RetentionManager(config_manager)
verifier       = DumpVerifier()

scheduler = BackgroundScheduler()
scheduler.start()

active_jobs    = {}   # sched_id -> APScheduler job
dump_progress  = {}   # dump_id  -> progress dict
cancel_flags   = {}   # dump_id  -> bool (True = cancel requested)


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
    db_name = db_config.get('database', 'dump')

    try:
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

        success = dumper.dump(filepath)

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
            size = os.path.getsize(actual_filepath) if os.path.exists(actual_filepath) else 0

            # Verification
            verify_result = None
            if settings.get('auto_verify', False):
                emit_progress(dump_id, {
                    'status': 'running', 'percent': 100,
                    'message': 'Verifying dump integrity…'
                })
                verify_result = verifier.verify(actual_filepath,
                                                db_type=db_config.get('type', 'postgresql'))

            history_item = {
                'dump_id':    dump_id,
                'db_id':      db_config.get('id'),
                'db_name':    db_name,
                'filename':   actual_filename,
                'filepath':   actual_filepath,
                'size':       size,
                'status':     'done',
                'created_at': datetime.now().isoformat(),
                'verify':     verify_result,
            }
            config_manager.add_history(history_item)

            emit_progress(dump_id, {
                'status':      'done',
                'percent':     100,
                'message':     f'Completed: {actual_filename}',
                'file':        actual_filepath,
                'filename':    actual_filename,
                'size':        size,
                'finished_at': datetime.now().isoformat(),
                'verify':      verify_result,
            })

            # Notifications
            notifier.notify('success', {
                'db_name':     db_name,
                'filename':    actual_filename,
                'size':        size,
                'finished_at': datetime.now().isoformat(),
            })

            # Retention policy
            retention_mgr.apply(db_config.get('id'))

        else:
            last_msg = dump_progress.get(dump_id, {}).get('message', 'Unknown error')
            emit_progress(dump_id, {
                'status':      'error',
                'percent':     0,
                'message':     last_msg,
                'finished_at': datetime.now().isoformat(),
            })
            notifier.notify('error', {
                'db_name':     db_name,
                'message':     last_msg,
                'finished_at': datetime.now().isoformat(),
            })

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

    def scheduled_dump():
        dump_id = str(uuid.uuid4())
        socketio.start_background_task(_run_dump_task, db, dump_id, save_path)

    job = scheduler.add_job(scheduled_dump, CronTrigger.from_crontab(data['cron']),
                            id=sched_id, replace_existing=True)
    active_jobs[sched_id] = job
    config_manager.add_schedule(data)
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
        save_path = (s.get('save_path') or
                     config_manager.get_settings().get('default_save_path', './dumps'))

        def make_job(db_cfg, sp):
            def fn():
                socketio.start_background_task(_run_dump_task, db_cfg, str(uuid.uuid4()), sp)
            return fn

        try:
            job = scheduler.add_job(make_job(db, save_path),
                                    CronTrigger.from_crontab(s['cron']),
                                    id=s['id'], replace_existing=True)
            active_jobs[s['id']] = job
        except Exception as e:
            logger.warning(f'Could not restore schedule {s["id"]}: {e}')


if __name__ == '__main__':
    restore_schedules()
    print('\n✅  DB Dump Manager running → http://127.0.0.1:5000\n')
    print(f'   Encryption: {"enabled ✓" if crypto.is_available() else "disabled (install cryptography)"}')
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)
    #socketio.run(app, host='127.0.0.1', port=5000, debug=False)