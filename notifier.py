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


class NotificationManager:
    def __init__(self, settings: dict):
        self.settings = settings or {}

    def _fmt_size(self, b: int) -> str:
        if not b:
            return '—'
        for unit in ('B', 'KB', 'MB', 'GB'):
            if b < 1024:
                return f'{b:.1f} {unit}'
            b /= 1024
        return f'{b:.2f} TB'

    def notify(self, event: str, dump_info: dict):
        """
        event: 'success' | 'error'
        dump_info: dict with db_name, filename, size, message, started_at, finished_at
        """
        cfg = self.settings.get('notifications', {})
        if not cfg.get('enabled'):
            return

        title, body = self._build_message(event, dump_info)

        if cfg.get('email', {}).get('enabled'):
            self._send_email(cfg['email'], title, body)

        if cfg.get('telegram', {}).get('enabled'):
            self._send_telegram(cfg['telegram'], title, body)

        if cfg.get('webhook', {}).get('enabled'):
            self._send_webhook(cfg['webhook'], event, dump_info, body)

    def _build_message(self, event: str, info: dict):
        icon = '✅' if event == 'success' else '❌'
        db   = info.get('db_name', '?')
        fn   = info.get('filename', '?')
        sz   = self._fmt_size(info.get('size', 0))
        msg  = info.get('message', '')
        ts   = info.get('finished_at', datetime.now().isoformat())[:19].replace('T', ' ')

        if event == 'success':
            title = f'{icon} DB Dump OK: {db}'
            body  = (f'Database:  {db}\n'
                     f'File:      {fn}\n'
                     f'Size:      {sz}\n'
                     f'Finished:  {ts}')
        else:
            title = f'{icon} DB Dump FAILED: {db}'
            body  = (f'Database:  {db}\n'
                     f'Error:     {msg}\n'
                     f'Time:      {ts}')
        return title, body

    # ── Email ──────────────────────────────────────────────────────────────────

    def _send_email(self, cfg: dict, title: str, body: str):
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

            html = f'<pre style="font-family:monospace">{body}</pre>'
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
                'event':    event,
                'db_name':  info.get('db_name'),
                'filename': info.get('filename'),
                'size':     info.get('size'),
                'message':  info.get('message'),
                'time':     info.get('finished_at'),
                'text':     body,
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
            'db_name':     'test_database',
            'filename':    'test_20240101_120000.sql',
            'size':        1024 * 1024 * 42,
            'message':     'Test notification',
            'finished_at': datetime.now().isoformat(),
        }
        cfg = self.settings.get('notifications', {})
        try:
            title, body = self._build_message('success', test_info)
            if channel == 'email':
                self._send_email(cfg.get('email', {}), title, body)
            elif channel == 'telegram':
                self._send_telegram(cfg.get('telegram', {}), title, body)
            elif channel == 'webhook':
                self._send_webhook(cfg.get('webhook', {}), 'success', test_info, body)
            return True, f'Test {channel} notification sent'
        except Exception as e:
            return False, str(e)
