"""
NotificationManager — sends alerts on dump success/failure
via Email (SMTP), Telegram Bot, or HTTP webhook.
"""

import json
import logging
import smtplib
import urllib.request
import urllib.parse
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

logger = logging.getLogger(__name__)


# ── Formatting helpers ────────────────────────────────────────────────────────

def _fmt_size(b: int) -> str:
    if not b:
        return '—'
    for unit in ('B', 'KB', 'MB', 'GB'):
        if b < 1024:
            return f'{b:.1f} {unit}'
        b /= 1024
    return f'{b:.2f} TB'


def _fmt_duration(seconds: float) -> str:
    if not seconds:
        return '—'
    s = int(seconds)
    if s < 60:
        return f'{s}s'
    m, s = divmod(s, 60)
    if m < 60:
        return f'{m}m {s}s'
    h, m = divmod(m, 60)
    return f'{h}h {m}m {s}s'


class NotificationManager:
    def __init__(self, settings: dict, digest_queue_mgr=None):
        self.settings = settings or {}
        self._digest_queue_mgr = digest_queue_mgr

    def _fmt_size(self, b: int) -> str:
        return _fmt_size(b)

    def _fmt_duration(self, seconds: float) -> str:
        return _fmt_duration(seconds)

    def notify(self, event: str, dump_info: dict):
        """
        event: 'success' | 'error'
        dump_info: dict with db_name, filename, size, message, started_at,
                   finished_at, duration_s, speed_mbps, rows_exported,
                   tables_exported, compression_method, compression_ratio,
                   cloud_url, db_host, db_type
        """
        cfg = self.settings.get('notifications', {})
        if not cfg.get('enabled'):
            return

        title, body, html_body = self._build_message(event, dump_info)

        if cfg.get('email', {}).get('enabled'):
            email_cfg = cfg['email']
            if email_cfg.get('frequency', 'per_dump') == 'daily_digest':
                self._enqueue_digest('email', event, dump_info)
            else:
                self._send_email(email_cfg, title, body, html_body)

        if cfg.get('telegram', {}).get('enabled'):
            tg_cfg = cfg['telegram']
            if tg_cfg.get('frequency', 'per_dump') == 'daily_digest':
                self._enqueue_digest('telegram', event, dump_info)
            else:
                self._send_telegram(tg_cfg, title, body)

        if cfg.get('webhook', {}).get('enabled'):
            self._send_webhook(cfg['webhook'], event, dump_info, body)

    def _build_message(self, event: str, info: dict):
        icon   = '✅' if event == 'success' else '❌'
        db     = info.get('db_name', '?')
        db_host = info.get('db_host', '')
        db_type = (info.get('db_type') or '').upper()
        fn     = info.get('filename', '?')
        sz     = self._fmt_size(info.get('size', 0))
        msg    = info.get('message', '')
        ts     = info.get('finished_at', datetime.now().isoformat())[:19].replace('T', ' ')

        if event == 'success':
            title = f'{icon} DB Dump OK: {db}'

            dur    = self._fmt_duration(info.get('duration_s'))
            speed  = info.get('speed_mbps', 0)
            speed_str = f'{speed} MB/s' if speed else '—'
            rows   = info.get('rows_exported', 0)
            tables = info.get('tables_exported', 0)
            comp   = info.get('compression_method')
            ratio  = info.get('compression_ratio')
            cloud  = info.get('cloud_url', '')
            uncomp_sz = self._fmt_size(info.get('uncompressed_size', 0))

            lines = [
                f'Database:   {db}',
            ]
            if db_type:
                lines.append(f'Type:       {db_type}')
            if db_host:
                lines.append(f'Host:       {db_host}')
            lines += [
                f'File:       {fn}',
                f'Size:       {sz}',
            ]
            if comp and comp != 'none' and info.get('uncompressed_size'):
                lines.append(f'Original:   {uncomp_sz}')
                lines.append(f'Compressed: {comp.upper()}' + (f' (ratio {ratio}x)' if ratio else ''))
            lines += [
                f'Duration:   {dur}',
                f'Speed:      {speed_str}',
            ]
            if tables:
                lines.append(f'Tables:     {tables}')
            if rows:
                lines.append(f'Rows:       {rows:,}')
            if cloud:
                lines.append(f'Cloud URL:  {cloud}')
            lines.append(f'Finished:   {ts}')

            body = '\n'.join(lines)
            html_body = _build_success_html(title, db, db_type, db_host, fn, sz,
                                            uncomp_sz, comp, ratio, dur, speed_str,
                                            tables, rows, cloud, ts)
        else:
            title = f'{icon} DB Dump FAILED: {db}'
            lines = [f'Database:   {db}']
            if db_type:
                lines.append(f'Type:       {db_type}')
            if db_host:
                lines.append(f'Host:       {db_host}')
            lines += [
                f'Error:      {msg}',
                f'Time:       {ts}',
            ]
            body = '\n'.join(lines)
            html_body = _build_error_html(title, db, db_type, db_host, msg, ts)

        return title, body, html_body

    # ── Digest queue ───────────────────────────────────────────────────────────

    def _enqueue_digest(self, channel: str, event: str, dump_info: dict):
        """Add a notification to the daily digest queue."""
        if self._digest_queue_mgr is None:
            logger.warning('Digest queue manager not set; falling back to immediate send')
            cfg = self.settings.get('notifications', {})
            title, body, html_body = self._build_message(event, dump_info)
            if channel == 'email':
                self._send_email(cfg.get('email', {}), title, body, html_body)
            elif channel == 'telegram':
                self._send_telegram(cfg.get('telegram', {}), title, body)
            return
        entry = {
            'channel':   channel,
            'event':     event,
            'dump_info': dump_info,
            'queued_at': datetime.now().isoformat(),
        }
        self._digest_queue_mgr.add_to_digest_queue(entry)
        logger.info(f'Queued {event} notification for daily {channel} digest')

    def send_daily_digest(self):
        """Send the daily digest for all queued notifications and clear the queue."""
        if self._digest_queue_mgr is None:
            return
        queue = self._digest_queue_mgr.get_digest_queue()
        if not queue:
            logger.info('Daily digest: queue is empty, nothing to send')
            return

        cfg = self.settings.get('notifications', {})
        if not cfg.get('enabled'):
            self._digest_queue_mgr.clear_digest_queue()
            return

        date_str = datetime.now().strftime('%Y-%m-%d')

        # Split by channel
        email_entries  = [e for e in queue if e['channel'] == 'email']
        tg_entries     = [e for e in queue if e['channel'] == 'telegram']

        email_cfg = cfg.get('email', {})
        if email_cfg.get('enabled') and email_entries:
            title    = f'📋 DB Dump Daily Digest — {date_str}'
            body     = self._build_digest_body(email_entries, date_str)
            html_body = _build_digest_html(email_entries, date_str)
            self._send_email(email_cfg, title, body, html_body)

        tg_cfg = cfg.get('telegram', {})
        if tg_cfg.get('enabled') and tg_entries:
            title = f'📋 DB Dump Daily Digest — {date_str}'
            body  = self._build_digest_body(tg_entries, date_str)
            self._send_telegram(tg_cfg, title, body)

        self._digest_queue_mgr.clear_digest_queue()
        logger.info(f'Daily digest sent: {len(email_entries)} email, {len(tg_entries)} telegram entries')

    def _build_digest_body(self, entries: list, date_str: str) -> str:
        """Build plain-text digest body from queued entries."""
        total   = len(entries)
        ok      = sum(1 for e in entries if e['event'] == 'success')
        failed  = total - ok
        lines   = [
            f'Daily Digest: {date_str}',
            f'Total dumps:  {total}  (✅ {ok} OK, ❌ {failed} failed)',
            '',
        ]
        for i, e in enumerate(entries, 1):
            info   = e.get('dump_info', {})
            icon   = '✅' if e['event'] == 'success' else '❌'
            db     = info.get('db_name', '?')
            ts     = (info.get('finished_at') or e.get('queued_at', '?'))[:19].replace('T', ' ')
            sz     = self._fmt_size(info.get('size', 0))
            dur    = self._fmt_duration(info.get('duration_s'))
            msg    = info.get('message', '')
            if e['event'] == 'success':
                lines.append(f'{i}. {icon} {db}  |  {sz}  |  {dur}  |  {ts}')
            else:
                lines.append(f'{i}. {icon} {db}  |  {msg[:60]}  |  {ts}')
        return '\n'.join(lines)

    # ── Email ──────────────────────────────────────────────────────────────────

    def _send_email(self, cfg: dict, title: str, body: str, html_body: str = ''):
        try:
            host     = cfg.get('smtp_host', 'smtp.gmail.com')
            port     = int(cfg.get('smtp_port', 587))
            user     = cfg.get('smtp_user', '')
            password = cfg.get('smtp_password', '')
            to       = cfg.get('to', '')
            use_tls  = cfg.get('use_tls', True)

            if not to:
                return

            msg = MIMEMultipart('alternative')
            msg['Subject'] = title
            msg['From']    = user
            msg['To']      = to

            html = html_body or f'<pre style="font-family:monospace;font-size:13px;line-height:1.6">{body}</pre>'
            msg.attach(MIMEText(body, 'plain'))
            msg.attach(MIMEText(html, 'html'))

            with smtplib.SMTP(host, port, timeout=15) as smtp:
                if use_tls:
                    smtp.starttls()
                if user and password:
                    smtp.login(user, password)
                smtp.sendmail(user, to.split(','), msg.as_string())

            logger.info(f'Email notification sent to {to}')
        except Exception as e:
            logger.error(f'Email notification failed: {e}')

    # ── Telegram ───────────────────────────────────────────────────────────────

    def _send_telegram(self, cfg: dict, title: str, body: str):
        try:
            token   = cfg.get('bot_token', '')
            chat_id = cfg.get('chat_id', '')
            if not token or not chat_id:
                return

            text = f'*{title}*\n```\n{body}\n```'
            url  = f'https://api.telegram.org/bot{token}/sendMessage'
            data = json.dumps({
                'chat_id':    chat_id,
                'text':       text,
                'parse_mode': 'Markdown',
            }).encode()

            req = urllib.request.Request(url, data=data,
                                         headers={'Content-Type': 'application/json'})
            with urllib.request.urlopen(req, timeout=15) as resp:
                pass
            logger.info('Telegram notification sent')
        except Exception as e:
            logger.error(f'Telegram notification failed: {e}')

    # ── Webhook ────────────────────────────────────────────────────────────────

    def _send_webhook(self, cfg: dict, event: str, info: dict, body: str):
        try:
            url = cfg.get('url', '')
            if not url:
                return

            payload = json.dumps({
                'event':              event,
                'db_name':            info.get('db_name'),
                'db_host':            info.get('db_host'),
                'db_type':            info.get('db_type'),
                'filename':           info.get('filename'),
                'size':               info.get('size'),
                'duration_s':         info.get('duration_s'),
                'speed_mbps':         info.get('speed_mbps'),
                'tables_exported':    info.get('tables_exported'),
                'rows_exported':      info.get('rows_exported'),
                'compression_method': info.get('compression_method'),
                'compression_ratio':  info.get('compression_ratio'),
                'cloud_url':          info.get('cloud_url'),
                'message':            info.get('message'),
                'time':               info.get('finished_at'),
                'text':               body,
            }).encode()

            req = urllib.request.Request(url, data=payload,
                                         headers={'Content-Type': 'application/json'})
            with urllib.request.urlopen(req, timeout=15) as resp:
                pass
            logger.info(f'Webhook notification sent to {url}')
        except Exception as e:
            logger.error(f'Webhook notification failed: {e}')

    # ── Test ───────────────────────────────────────────────────────────────────

    def test(self, channel: str) -> tuple:
        """Send a test notification. Returns (ok, message)."""
        test_info = {
            'db_name':            'test_database',
            'db_host':            'db.example.com',
            'db_type':            'postgresql',
            'filename':           'test_20240101_120000.sql',
            'size':               1024 * 1024 * 42,
            'uncompressed_size':  1024 * 1024 * 85,
            'duration_s':         37.5,
            'speed_mbps':         2.27,
            'rows_exported':      158432,
            'tables_exported':    12,
            'compression_method': 'zstd',
            'compression_ratio':  2.02,
            'cloud_url':          '',
            'message':            'Test notification',
            'finished_at':        datetime.now().isoformat(),
        }
        cfg = self.settings.get('notifications', {})
        try:
            title, body, html_body = self._build_message('success', test_info)
            if channel == 'email':
                self._send_email(cfg.get('email', {}), title, body, html_body)
            elif channel == 'telegram':
                self._send_telegram(cfg.get('telegram', {}), title, body)
            elif channel == 'webhook':
                self._send_webhook(cfg.get('webhook', {}), 'success', test_info, body)
            return True, f'Test {channel} notification sent'
        except Exception as e:
            return False, str(e)


# ── HTML email builders ───────────────────────────────────────────────────────

def _esc(s) -> str:
    """Minimal HTML escaping."""
    return str(s).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


def _build_success_html(title, db, db_type, db_host, fn, sz, uncomp_sz,
                        comp, ratio, dur, speed_str, tables, rows, cloud, ts) -> str:
    rows_str   = f'{rows:,}' if rows else '—'
    tables_str = str(tables) if tables else '—'
    comp_row   = ''
    if comp and comp != 'none':
        ratio_str = f' (ratio {ratio}x)' if ratio else ''
        comp_row = f'''
        <tr><td style="color:#6b7a99;padding:5px 10px">Original size</td>
            <td style="padding:5px 10px;font-family:monospace">{_esc(uncomp_sz)}</td></tr>
        <tr><td style="color:#6b7a99;padding:5px 10px">Compression</td>
            <td style="padding:5px 10px;font-family:monospace">{_esc(comp.upper())}{_esc(ratio_str)}</td></tr>'''
    cloud_row = ''
    if cloud:
        cloud_row = f'''
        <tr><td style="color:#6b7a99;padding:5px 10px">Cloud URL</td>
            <td style="padding:5px 10px;font-family:monospace"><a href="{_esc(cloud)}">{_esc(cloud)}</a></td></tr>'''
    type_row = f'<tr><td style="color:#6b7a99;padding:5px 10px">Type</td><td style="padding:5px 10px;font-family:monospace">{_esc(db_type)}</td></tr>' if db_type else ''
    host_row = f'<tr><td style="color:#6b7a99;padding:5px 10px">Host</td><td style="padding:5px 10px;font-family:monospace">{_esc(db_host)}</td></tr>' if db_host else ''
    return f'''<!DOCTYPE html><html><body style="margin:0;padding:0;background:#f0f2f7;font-family:Inter,Arial,sans-serif">
<div style="max-width:520px;margin:32px auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 16px rgba(0,0,0,.08)">
  <div style="background:#0ea271;padding:20px 28px">
    <div style="font-size:22px;margin-bottom:4px">✅</div>
    <div style="color:#fff;font-size:17px;font-weight:700">{_esc(title)}</div>
  </div>
  <div style="padding:24px 28px">
    <table style="width:100%;border-collapse:collapse;font-size:13px">
      <tr><td style="color:#6b7a99;padding:5px 10px">Database</td>
          <td style="padding:5px 10px;font-weight:600">{_esc(db)}</td></tr>
      {type_row}
      {host_row}
      <tr style="background:#f5f7fb"><td style="color:#6b7a99;padding:5px 10px">File</td>
          <td style="padding:5px 10px;font-family:monospace;font-size:12px">{_esc(fn)}</td></tr>
      <tr><td style="color:#6b7a99;padding:5px 10px">Size</td>
          <td style="padding:5px 10px;font-family:monospace">{_esc(sz)}</td></tr>
      {comp_row}
      <tr style="background:#f5f7fb"><td style="color:#6b7a99;padding:5px 10px">Duration</td>
          <td style="padding:5px 10px;font-family:monospace">{_esc(dur)}</td></tr>
      <tr><td style="color:#6b7a99;padding:5px 10px">Speed</td>
          <td style="padding:5px 10px;font-family:monospace">{_esc(speed_str)}</td></tr>
      <tr style="background:#f5f7fb"><td style="color:#6b7a99;padding:5px 10px">Tables</td>
          <td style="padding:5px 10px;font-family:monospace">{_esc(tables_str)}</td></tr>
      <tr><td style="color:#6b7a99;padding:5px 10px">Rows</td>
          <td style="padding:5px 10px;font-family:monospace">{_esc(rows_str)}</td></tr>
      {cloud_row}
      <tr style="background:#f5f7fb"><td style="color:#6b7a99;padding:5px 10px">Finished</td>
          <td style="padding:5px 10px;font-family:monospace">{_esc(ts)}</td></tr>
    </table>
  </div>
  <div style="background:#f5f7fb;padding:12px 28px;font-size:11px;color:#8892aa;border-top:1px solid #dde2ee">
    DB Dump Manager — automated backup notification
  </div>
</div>
</body></html>'''


def _build_error_html(title, db, db_type, db_host, msg, ts) -> str:
    type_row = f'<tr><td style="color:#6b7a99;padding:5px 10px">Type</td><td style="padding:5px 10px;font-family:monospace">{_esc(db_type)}</td></tr>' if db_type else ''
    host_row = f'<tr><td style="color:#6b7a99;padding:5px 10px">Host</td><td style="padding:5px 10px;font-family:monospace">{_esc(db_host)}</td></tr>' if db_host else ''
    return f'''<!DOCTYPE html><html><body style="margin:0;padding:0;background:#f0f2f7;font-family:Inter,Arial,sans-serif">
<div style="max-width:520px;margin:32px auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 16px rgba(0,0,0,.08)">
  <div style="background:#e0394a;padding:20px 28px">
    <div style="font-size:22px;margin-bottom:4px">❌</div>
    <div style="color:#fff;font-size:17px;font-weight:700">{_esc(title)}</div>
  </div>
  <div style="padding:24px 28px">
    <table style="width:100%;border-collapse:collapse;font-size:13px">
      <tr><td style="color:#6b7a99;padding:5px 10px">Database</td>
          <td style="padding:5px 10px;font-weight:600">{_esc(db)}</td></tr>
      {type_row}
      {host_row}
      <tr style="background:#f5f7fb"><td style="color:#6b7a99;padding:5px 10px;vertical-align:top">Error</td>
          <td style="padding:5px 10px;font-family:monospace;font-size:12px;color:#e0394a;word-break:break-word">{_esc(msg)}</td></tr>
      <tr><td style="color:#6b7a99;padding:5px 10px">Time</td>
          <td style="padding:5px 10px;font-family:monospace">{_esc(ts)}</td></tr>
    </table>
  </div>
  <div style="background:#f5f7fb;padding:12px 28px;font-size:11px;color:#8892aa;border-top:1px solid #dde2ee">
    DB Dump Manager — automated backup notification
  </div>
</div>
</body></html>'''


def _build_digest_html(entries: list, date_str: str) -> str:
    """Build an HTML email for the daily digest."""
    total  = len(entries)
    ok     = sum(1 for e in entries if e['event'] == 'success')
    failed = total - ok

    rows_html = ''
    for i, e in enumerate(entries):
        info    = e.get('dump_info', {})
        event   = e['event']
        icon    = '✅' if event == 'success' else '❌'
        db      = _esc(info.get('db_name', '?'))
        ts      = _esc((info.get('finished_at') or e.get('queued_at', '?'))[:19].replace('T', ' '))
        bg      = '#fff' if i % 2 == 0 else '#f5f7fb'
        status_color = '#0ea271' if event == 'success' else '#e0394a'

        if event == 'success':
            sz  = _esc(_fmt_size(info.get('size', 0)))
            dur = _esc(_fmt_duration(info.get('duration_s')))
            detail = f'{sz} · {dur}'
        else:
            detail = _esc((info.get('message') or 'error')[:60])

        rows_html += f'''
        <tr style="background:{bg}">
          <td style="padding:6px 10px;font-size:18px">{icon}</td>
          <td style="padding:6px 10px;font-weight:600;color:{status_color}">{db}</td>
          <td style="padding:6px 10px;font-family:monospace;font-size:12px;color:#6b7a99">{detail}</td>
          <td style="padding:6px 10px;font-family:monospace;font-size:11px;color:#8892aa">{ts}</td>
        </tr>'''

    return f'''<!DOCTYPE html><html><body style="margin:0;padding:0;background:#f0f2f7;font-family:Inter,Arial,sans-serif">
<div style="max-width:620px;margin:32px auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 16px rgba(0,0,0,.08)">
  <div style="background:#3b6ef5;padding:20px 28px">
    <div style="font-size:22px;margin-bottom:4px">📋</div>
    <div style="color:#fff;font-size:17px;font-weight:700">DB Dump Daily Digest — {_esc(date_str)}</div>
  </div>
  <div style="padding:20px 28px">
    <div style="display:flex;gap:24px;margin-bottom:20px">
      <div style="flex:1;background:#f5f7fb;border-radius:8px;padding:12px 16px;text-align:center">
        <div style="font-size:24px;font-weight:700">{total}</div>
        <div style="font-size:12px;color:#6b7a99;margin-top:2px">Total dumps</div>
      </div>
      <div style="flex:1;background:#f0faf5;border-radius:8px;padding:12px 16px;text-align:center">
        <div style="font-size:24px;font-weight:700;color:#0ea271">{ok}</div>
        <div style="font-size:12px;color:#6b7a99;margin-top:2px">Successful</div>
      </div>
      <div style="flex:1;background:#fff5f6;border-radius:8px;padding:12px 16px;text-align:center">
        <div style="font-size:24px;font-weight:700;color:#e0394a">{failed}</div>
        <div style="font-size:12px;color:#6b7a99;margin-top:2px">Failed</div>
      </div>
    </div>
    <table style="width:100%;border-collapse:collapse;font-size:13px">
      <thead>
        <tr style="background:#f5f7fb">
          <th style="padding:6px 10px;text-align:left;color:#6b7a99;font-weight:600;font-size:11px"></th>
          <th style="padding:6px 10px;text-align:left;color:#6b7a99;font-weight:600;font-size:11px">Database</th>
          <th style="padding:6px 10px;text-align:left;color:#6b7a99;font-weight:600;font-size:11px">Details</th>
          <th style="padding:6px 10px;text-align:left;color:#6b7a99;font-weight:600;font-size:11px">Time</th>
        </tr>
      </thead>
      <tbody>{rows_html}
      </tbody>
    </table>
  </div>
  <div style="background:#f5f7fb;padding:12px 28px;font-size:11px;color:#8892aa;border-top:1px solid #dde2ee">
    DB Dump Manager — daily digest notification
  </div>
</div>
</body></html>'''
