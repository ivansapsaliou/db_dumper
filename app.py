import os
import json
import uuid
import threading
import shutil
import time
import logging
from datetime import datetime
from pathlib import Path

from flask import Flask, render_template, request, jsonify, send_file, abort
from flask_socketio import SocketIO, emit
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import psutil

from db_dumper import DatabaseDumper
from config_manager import ConfigManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.urandom(24)
socketio = SocketIO(app, async_mode='eventlet', cors_allowed_origins='*')

config_manager = ConfigManager('config.json')
scheduler = BackgroundScheduler()
scheduler.start()

active_jobs = {}      # job_id -> job info
dump_progress = {}    # dump_id -> progress info


def emit_progress(dump_id, data):
    dump_progress[dump_id] = data
    socketio.emit('progress', {'dump_id': dump_id, **data})


def run_dump(db_config, dump_id, save_path):
    try:
        emit_progress(dump_id, {
            'status': 'running',
            'percent': 0,
            'message': 'Initializing dump...',
            'started_at': datetime.now().isoformat()
        })

        dumper = DatabaseDumper(db_config, socketio, dump_id, emit_progress)

        # Check disk space
        required_mb = db_config.get('estimated_size_mb', 500)
        free_mb = shutil.disk_usage(save_path).free / (1024 * 1024)
        if free_mb < required_mb * 1.2:
            emit_progress(dump_id, {
                'status': 'error',
                'percent': 0,
                'message': f'Not enough disk space. Required: ~{required_mb}MB, Available: {free_mb:.0f}MB'
            })
            return

        os.makedirs(save_path, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        db_name = db_config.get('database', 'dump')
        filename = f"{db_name}_{timestamp}.sql"
        filepath = os.path.join(save_path, filename)

        success = dumper.dump(filepath)

        if success:
            size = os.path.getsize(filepath) if os.path.exists(filepath) else 0
            emit_progress(dump_id, {
                'status': 'done',
                'percent': 100,
                'message': f'Dump completed: {filename}',
                'file': filepath,
                'filename': filename,
                'size': size,
                'finished_at': datetime.now().isoformat()
            })
            # Save to history
            config_manager.add_history({
                'dump_id': dump_id,
                'db_id': db_config.get('id'),
                'db_name': db_name,
                'filename': filename,
                'filepath': filepath,
                'size': size,
                'status': 'done',
                'created_at': datetime.now().isoformat()
            })
        else:
            emit_progress(dump_id, {
                'status': 'error',
                'percent': 0,
                'message': 'Dump failed. Check logs.',
                'finished_at': datetime.now().isoformat()
            })

    except Exception as e:
        logger.exception(f"Dump error: {e}")
        emit_progress(dump_id, {
            'status': 'error',
            'percent': 0,
            'message': str(e),
            'finished_at': datetime.now().isoformat()
        })


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/databases', methods=['GET'])
def get_databases():
    return jsonify(config_manager.get_databases())


@app.route('/api/databases', methods=['POST'])
def add_database():
    data = request.json
    data['id'] = str(uuid.uuid4())
    config_manager.add_database(data)
    return jsonify({'ok': True, 'id': data['id']})


@app.route('/api/databases/<db_id>', methods=['PUT'])
def update_database(db_id):
    data = request.json
    config_manager.update_database(db_id, data)
    return jsonify({'ok': True})


@app.route('/api/databases/<db_id>', methods=['DELETE'])
def delete_database(db_id):
    config_manager.delete_database(db_id)
    return jsonify({'ok': True})


@app.route('/api/databases/<db_id>/test', methods=['POST'])
def test_connection(db_id):
    db = config_manager.get_database(db_id)
    if not db:
        return jsonify({'ok': False, 'message': 'Database not found'}), 404
    dumper = DatabaseDumper(db, socketio, None, None)
    ok, msg = dumper.test_connection()
    return jsonify({'ok': ok, 'message': msg})


@app.route('/api/dump/start', methods=['POST'])
def start_dump():
    data = request.json
    db_id = data.get('db_id')
    db = config_manager.get_database(db_id)
    if not db:
        return jsonify({'ok': False, 'message': 'Database not found'}), 404

    save_path = data.get('save_path') or config_manager.get_settings().get('default_save_path', './dumps')
    dump_id = str(uuid.uuid4())

    t = threading.Thread(target=run_dump, args=(db, dump_id, save_path), daemon=True)
    t.start()

    return jsonify({'ok': True, 'dump_id': dump_id})


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


@app.route('/api/history')
def get_history():
    return jsonify(config_manager.get_history())


@app.route('/api/history/download/<filename>')
def download_history_file(filename):
    history = config_manager.get_history()
    for item in history:
        if item.get('filename') == filename:
            fp = item.get('filepath')
            if fp and os.path.exists(fp):
                return send_file(fp, as_attachment=True, download_name=filename)
    abort(404)


@app.route('/api/history/<dump_id>', methods=['DELETE'])
def delete_history(dump_id):
    config_manager.delete_history(dump_id)
    return jsonify({'ok': True})


@app.route('/api/settings', methods=['GET'])
def get_settings():
    return jsonify(config_manager.get_settings())


@app.route('/api/settings', methods=['POST'])
def save_settings():
    config_manager.save_settings(request.json)
    return jsonify({'ok': True})


@app.route('/api/disk', methods=['POST'])
def check_disk():
    path = request.json.get('path', '.')
    try:
        usage = shutil.disk_usage(path)
        return jsonify({
            'ok': True,
            'total_gb': round(usage.total / 1e9, 2),
            'used_gb': round(usage.used / 1e9, 2),
            'free_gb': round(usage.free / 1e9, 2),
            'percent': round(usage.used / usage.total * 100, 1)
        })
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)})


@app.route('/api/schedules', methods=['GET'])
def get_schedules():
    return jsonify(config_manager.get_schedules())


@app.route('/api/schedules', methods=['POST'])
def add_schedule():
    data = request.json
    sched_id = str(uuid.uuid4())
    data['id'] = sched_id
    data['enabled'] = True

    db = config_manager.get_database(data['db_id'])
    save_path = data.get('save_path') or config_manager.get_settings().get('default_save_path', './dumps')

    def scheduled_dump():
        dump_id = str(uuid.uuid4())
        run_dump(db, dump_id, save_path)

    job = scheduler.add_job(
        scheduled_dump,
        CronTrigger.from_crontab(data['cron']),
        id=sched_id,
        replace_existing=True
    )
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
            if s['enabled']:
                try:
                    scheduler.resume_job(sched_id)
                except Exception:
                    pass
            else:
                try:
                    scheduler.pause_job(sched_id)
                except Exception:
                    pass
            config_manager.save_schedules(schedules)
            return jsonify({'ok': True, 'enabled': s['enabled']})
    return jsonify({'ok': False}), 404


# ── Restore saved schedules on startup ────────────────────────────────────────
def restore_schedules():
    for s in config_manager.get_schedules():
        if not s.get('enabled', True):
            continue
        db = config_manager.get_database(s['db_id'])
        if not db:
            continue
        save_path = s.get('save_path') or config_manager.get_settings().get('default_save_path', './dumps')

        def make_job(db_cfg, sp):
            def fn():
                dump_id = str(uuid.uuid4())
                run_dump(db_cfg, dump_id, sp)
            return fn

        try:
            job = scheduler.add_job(
                make_job(db, save_path),
                CronTrigger.from_crontab(s['cron']),
                id=s['id'],
                replace_existing=True
            )
            active_jobs[s['id']] = job
        except Exception as e:
            logger.warning(f"Could not restore schedule {s['id']}: {e}")


if __name__ == '__main__':
    restore_schedules()
    import eventlet
    import eventlet.wsgi
    print("\n✅  DB Dump Manager running → http://127.0.0.1:5000\n")
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)
