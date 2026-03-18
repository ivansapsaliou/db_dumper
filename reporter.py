"""
Reporter — backup reporting & analytics.

Features:
  - PDF weekly/monthly summary reports (requires reportlab)
  - CSV export of backup history
  - Compliance report: SLA & retention validation
  - Size/speed trend data
  - Performance metrics: dump speed (MB/min), duration
"""

import io
import csv
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# ── helper ────────────────────────────────────────────────────────────────────

def _mb(b: int) -> float:
    return round(b / 1_048_576, 2) if b else 0.0


# ── analytics helpers ─────────────────────────────────────────────────────────

def compute_analytics(history: list) -> dict:
    """
    Compute rich analytics from the backup history list.

    Returns a dict with:
      trends       — list of {date, size_mb, db_name, duration_s, speed_mbmin}
      by_db        — dict of db_name -> {count, total_size, avg_size, avg_speed}
      success_rate — 0-100
      sla          — {passed, failed, rate}
      top_errors   — list of {message, count}
    """
    if not history:
        return {
            'trends': [], 'by_db': {}, 'success_rate': 0,
            'sla': {'passed': 0, 'failed': 0, 'rate': 0},
            'top_errors': [],
        }

    done_items   = [h for h in history if h.get('status') == 'done']
    error_items  = [h for h in history if h.get('status') == 'error']
    total        = len(history)
    done_count   = len(done_items)

    # Trends (last 60 dumps)
    trends = []
    for h in reversed(history[:60]):
        size_mb  = _mb(h.get('size', 0))
        dur_s    = h.get('duration_s', 0) or 0
        speed    = round(size_mb / (dur_s / 60), 2) if dur_s and dur_s > 0 else 0
        trends.append({
            'date':        (h.get('created_at') or '')[:10],
            'size_mb':     size_mb,
            'db_name':     h.get('db_name', ''),
            'duration_s':  round(dur_s, 1),
            'speed_mbmin': speed,
            'status':      h.get('status', ''),
        })

    # Per-DB stats
    by_db: dict[str, dict] = {}
    for h in done_items:
        name    = h.get('db_name', '?')
        size_mb = _mb(h.get('size', 0))
        dur_s   = h.get('duration_s', 0) or 0
        speed   = round(size_mb / (dur_s / 60), 2) if dur_s and dur_s > 0 else 0
        if name not in by_db:
            by_db[name] = {'count': 0, 'total_size_mb': 0.0,
                           'total_speed': 0.0, 'speed_count': 0}
        by_db[name]['count']      += 1
        by_db[name]['total_size_mb'] += size_mb
        if speed > 0:
            by_db[name]['total_speed']  += speed
            by_db[name]['speed_count']  += 1

    by_db_clean = {}
    for name, d in by_db.items():
        avg_speed = (d['total_speed'] / d['speed_count']
                     if d['speed_count'] else 0)
        by_db_clean[name] = {
            'count':       d['count'],
            'total_size_mb': round(d['total_size_mb'], 2),
            'avg_size_mb': round(d['total_size_mb'] / d['count'], 2) if d['count'] else 0,
            'avg_speed_mbmin': round(avg_speed, 2),
        }

    # Top errors
    err_counts: dict[str, int] = {}
    for h in error_items:
        msg = (h.get('message') or 'Unknown error')[:80]
        err_counts[msg] = err_counts.get(msg, 0) + 1
    top_errors = sorted(
        [{'message': k, 'count': v} for k, v in err_counts.items()],
        key=lambda x: x['count'], reverse=True
    )[:10]

    return {
        'trends':       trends,
        'by_db':        by_db_clean,
        'success_rate': round(done_count / total * 100, 1) if total else 0,
        'sla': {
            'passed': done_count,
            'failed': total - done_count,
            'rate':   round(done_count / total * 100, 1) if total else 0,
        },
        'top_errors': top_errors,
    }


def get_summary(history: list, period_days: int = 30) -> dict:
    """Return a human-readable summary dict for the last period_days."""
    cutoff = (datetime.utcnow() - timedelta(days=period_days)).isoformat()
    recent = [h for h in history if (h.get('created_at') or '') >= cutoff]

    done   = [h for h in recent if h.get('status') == 'done']
    errors = [h for h in recent if h.get('status') == 'error']
    total_size = sum(h.get('size', 0) for h in done)

    durations = [h.get('duration_s', 0) or 0 for h in done if h.get('duration_s')]
    avg_dur   = round(sum(durations) / len(durations), 1) if durations else 0

    return {
        'period_days':   period_days,
        'total_dumps':   len(recent),
        'successful':    len(done),
        'failed':        len(errors),
        'success_rate':  round(len(done) / len(recent) * 100, 1) if recent else 0,
        'total_size_mb': _mb(total_size),
        'avg_duration_s': avg_dur,
        'databases':     len({h.get('db_name') for h in recent}),
        'generated_at':  datetime.utcnow().isoformat() + 'Z',
    }


# ── CSV export ────────────────────────────────────────────────────────────────

def export_csv(history: list) -> bytes:
    """Return CSV bytes of the full history."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        'dump_id', 'db_name', 'filename', 'status',
        'size_bytes', 'size_mb', 'created_at', 'duration_s',
        'speed_mbmin', 'cloud_url', 'verify_ok',
    ])
    for h in history:
        size_bytes = h.get('size', 0) or 0
        size_mb    = _mb(size_bytes)
        dur_s      = h.get('duration_s', 0) or 0
        speed      = round(size_mb / (dur_s / 60), 2) if dur_s and dur_s > 0 else ''
        verify     = ''
        if isinstance(h.get('verify'), dict):
            verify = 'ok' if h['verify'].get('ok') else 'fail'
        writer.writerow([
            h.get('dump_id', ''),
            h.get('db_name', ''),
            h.get('filename', ''),
            h.get('status', ''),
            size_bytes,
            size_mb,
            h.get('created_at', ''),
            round(dur_s, 1) if dur_s else '',
            speed,
            h.get('cloud_url', ''),
            verify,
        ])
    return buf.getvalue().encode('utf-8')


# ── PDF export ────────────────────────────────────────────────────────────────

def export_pdf(history: list, period_days: int = 30,
               title: Optional[str] = None) -> Optional[bytes]:
    """
    Generate a PDF backup summary report.
    Returns bytes or None if reportlab is not installed.
    """
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import cm
        from reportlab.lib import colors
        from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                        Table, TableStyle, HRFlowable)
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.enums import TA_CENTER, TA_RIGHT
    except ImportError:
        logger.warning('reportlab not installed — PDF export not available')
        return None

    summary = get_summary(history, period_days)
    analytics = compute_analytics(history)
    buf = io.BytesIO()

    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2*cm, bottomMargin=2*cm,
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('title', parent=styles['Title'],
                                 fontSize=18, spaceAfter=6)
    h2_style    = ParagraphStyle('h2', parent=styles['Heading2'],
                                 fontSize=12, spaceAfter=4, textColor=colors.HexColor('#3b6ef5'))
    body_style  = styles['BodyText']
    muted_style = ParagraphStyle('muted', parent=body_style,
                                 textColor=colors.HexColor('#8892aa'), fontSize=9)

    report_title = title or f'DB Dump Manager — Backup Report ({period_days}d)'
    elements = [
        Paragraph(report_title, title_style),
        Paragraph(f'Generated: {datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}',
                  muted_style),
        Spacer(1, 0.4*cm),
        HRFlowable(width='100%', thickness=1, color=colors.HexColor('#dde2ee')),
        Spacer(1, 0.3*cm),

        Paragraph('Summary', h2_style),
    ]

    # Summary table
    summary_data = [
        ['Period', f'{period_days} days'],
        ['Total dumps', str(summary['total_dumps'])],
        ['Successful', str(summary['successful'])],
        ['Failed', str(summary['failed'])],
        ['Success rate', f"{summary['success_rate']}%"],
        ['Total size', f"{summary['total_size_mb']} MB"],
        ['Avg duration', f"{summary['avg_duration_s']} s"],
        ['Databases', str(summary['databases'])],
    ]
    tbl = Table(summary_data, colWidths=[5*cm, 7*cm])
    tbl.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f5f7fb')),
        ('FONTNAME',   (0, 0), (-1, -1), 'Helvetica'),
        ('FONTSIZE',   (0, 0), (-1, -1), 10),
        ('GRID',       (0, 0), (-1, -1), 0.5, colors.HexColor('#dde2ee')),
        ('ROWBACKGROUNDS', (0, 0), (-1, -1),
         [colors.white, colors.HexColor('#f5f7fb')]),
        ('TOPPADDING',  (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
    ]))
    elements += [tbl, Spacer(1, 0.4*cm)]

    # Per-DB breakdown
    if analytics['by_db']:
        elements.append(Paragraph('Per-Database Breakdown', h2_style))
        header = ['Database', 'Dumps', 'Total (MB)', 'Avg (MB)', 'Avg Speed (MB/min)']
        rows   = [header]
        for name, d in list(analytics['by_db'].items())[:20]:
            rows.append([
                name, str(d['count']),
                str(d['total_size_mb']), str(d['avg_size_mb']),
                str(d['avg_speed_mbmin']),
            ])
        db_tbl = Table(rows, colWidths=[5*cm, 2*cm, 3*cm, 3*cm, 4*cm])
        db_tbl.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3b6ef5')),
            ('TEXTCOLOR',  (0, 0), (-1, 0), colors.white),
            ('FONTNAME',   (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTNAME',   (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE',   (0, 0), (-1, -1), 9),
            ('GRID',       (0, 0), (-1, -1), 0.5, colors.HexColor('#dde2ee')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1),
             [colors.white, colors.HexColor('#f5f7fb')]),
            ('TOPPADDING',    (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]))
        elements += [db_tbl, Spacer(1, 0.4*cm)]

    # Recent errors
    if analytics['top_errors']:
        elements.append(Paragraph('Top Errors', h2_style))
        err_rows = [['Error message', 'Count']]
        for e in analytics['top_errors'][:10]:
            err_rows.append([e['message'], str(e['count'])])
        err_tbl = Table(err_rows, colWidths=[13*cm, 2*cm])
        err_tbl.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#e0394a')),
            ('TEXTCOLOR',  (0, 0), (-1, 0), colors.white),
            ('FONTNAME',   (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTNAME',   (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE',   (0, 0), (-1, -1), 9),
            ('GRID',       (0, 0), (-1, -1), 0.5, colors.HexColor('#dde2ee')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1),
             [colors.white, colors.HexColor('#fef0f1')]),
            ('TOPPADDING',    (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]))
        elements += [err_tbl, Spacer(1, 0.4*cm)]

    doc.build(elements)
    return buf.getvalue()


# ── compliance report ─────────────────────────────────────────────────────────

def compliance_report(history: list, settings: dict) -> dict:
    """
    Check backup compliance against SLA / retention settings.
    Returns:
      {
        'sla_ok': bool,
        'retention_ok': bool,
        'issues': [str, ...],
        'databases': {name: {last_backup, age_h, sla_ok, ...}},
      }
    """
    retention_cfg = settings.get('retention', {})
    max_age_h = float(retention_cfg.get('max_age_hours', 24))
    issues = []

    # Group last backup by database
    last_backup: dict[str, dict] = {}
    for h in history:
        name = h.get('db_name', '?')
        if name not in last_backup or (h.get('created_at', '') >
                                        last_backup[name].get('created_at', '')):
            last_backup[name] = h

    databases = {}
    sla_ok     = True
    now        = datetime.utcnow()

    for name, h in last_backup.items():
        created = h.get('created_at', '')
        age_h: float = 9999
        if created:
            try:
                dt  = datetime.fromisoformat(created.rstrip('Z'))
                age_h = (now - dt).total_seconds() / 3600
            except ValueError:
                pass
        ok = h.get('status') == 'done' and age_h <= max_age_h
        if not ok:
            sla_ok = False
            if age_h > max_age_h:
                issues.append(f'{name}: last backup {age_h:.1f}h ago (SLA={max_age_h}h)')
            if h.get('status') != 'done':
                issues.append(f'{name}: last backup status = {h.get("status")}')
        databases[name] = {
            'last_backup':  created,
            'age_h':        round(age_h, 1),
            'last_status':  h.get('status', '?'),
            'sla_ok':       ok,
        }

    # Retention check
    keep_n      = int(retention_cfg.get('keep_last_n', 0))
    retention_ok = True
    if keep_n:
        done_count = sum(1 for h in history if h.get('status') == 'done')
        if done_count > keep_n * 1.5:
            retention_ok = False
            issues.append(
                f'Retention: {done_count} backups exist, policy keeps {keep_n}'
            )

    return {
        'sla_ok':       sla_ok,
        'retention_ok': retention_ok,
        'issues':       issues,
        'databases':    databases,
        'generated_at': now.isoformat() + 'Z',
    }
