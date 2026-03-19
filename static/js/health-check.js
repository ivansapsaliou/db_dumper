/**
 * health-check.js — Health Check Dashboard logic for DB Dump Manager
 *
 * Fetches /api/health/databases, renders status cards,
 * supports manual & auto-refresh, sort/filter by status.
 */

const HealthDashboard = (() => {
  let _data = {};           // db_id -> result
  let _autoTimer = null;
  let _autoInterval = 60;   // seconds
  let _sortBy = 'status';   // 'status' | 'name' | 'response_time'
  let _filterStatus = 'all'; // 'all' | 'ok' | 'down'

  // ── API ─────────────────────────────────────────────────────────────────────

  async function fetchAll() {
    const res = await fetch('/api/health/databases');
    if (!res.ok) throw new Error('Failed to fetch health status');
    return res.json();
  }

  async function fetchOne(dbId) {
    const res = await fetch(`/api/health/check/${dbId}`, { method: 'POST' });
    if (!res.ok) throw new Error('Failed to check database');
    return res.json();
  }

  // ── Rendering ────────────────────────────────────────────────────────────────

  function _formatMs(ms) {
    if (ms == null) return '—';
    if (ms < 1000) return ms.toFixed(0) + ' ms';
    return (ms / 1000).toFixed(2) + ' s';
  }

  function _timeAgo(iso) {
    if (!iso) return '—';
    const diff = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
    if (diff < 5)  return 'just now';
    if (diff < 60) return diff + 's ago';
    if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
    return Math.floor(diff / 3600) + 'h ago';
  }

  function _sorted(entries) {
    return entries.slice().sort((a, b) => {
      if (_sortBy === 'name')          return (a[1].name || a[0]).localeCompare(b[1].name || b[0]);
      if (_sortBy === 'response_time') return (a[1].response_time_ms || 0) - (b[1].response_time_ms || 0);
      // 'status': down first
      if (!a[1].ok && b[1].ok) return -1;
      if (a[1].ok && !b[1].ok) return  1;
      return (a[1].name || a[0]).localeCompare(b[1].name || b[0]);
    });
  }

  function render(container) {
    if (!container) return;
    const entries = Object.entries(_data);

    const filtered = entries.filter(([id, r]) => {
      if (_filterStatus === 'ok')   return r.ok;
      if (_filterStatus === 'down') return !r.ok;
      return true;
    });

    const total   = entries.length;
    const healthy = entries.filter(([, r]) => r.ok).length;
    const down    = total - healthy;

    // Summary strip
    const summaryEl = document.getElementById('hc-summary');
    if (summaryEl) {
      summaryEl.innerHTML =
        `<span class="hc-stat"><b>${total}</b> databases</span>` +
        `<span class="hc-stat ok"><b>${healthy}</b> healthy</span>` +
        `<span class="hc-stat ${down ? 'down' : ''}"><b>${down}</b> down</span>`;
    }

    if (!filtered.length) {
      container.innerHTML = '<div class="empty-state">No databases match the current filter.</div>';
      return;
    }

    const cards = _sorted(filtered).map(([dbId, r]) => {
      const statusClass = r.ok ? 'hc-ok' : 'hc-down';
      const indicator   = r.ok
        ? '<span class="hc-indicator ok" title="Online">●</span>'
        : '<span class="hc-indicator down" title="Down">●</span>';
      const dbTypeUpper = (r.db_type || '').toUpperCase() || 'DB';
      return `
        <div class="hc-card ${statusClass}" data-id="${_esc(dbId)}">
          <div class="hc-card-header">
            ${indicator}
            <span class="hc-db-name">${_esc(r.name || dbId)}</span>
            <span class="hc-db-type">${_esc(dbTypeUpper)}</span>
            <button class="btn btn-ghost btn-sm hc-refresh-btn" data-db-id="${_esc(dbId)}" title="Refresh this database">↻</button>
          </div>
          <div class="hc-card-body">
            <div class="hc-row">
              <span class="hc-label">Status</span>
              <span class="pill ${r.ok ? 'done' : 'error'}">${r.ok ? 'Online' : 'Down'}</span>
            </div>
            <div class="hc-row">
              <span class="hc-label">Response</span>
              <span class="hc-value">${_formatMs(r.response_time_ms)}</span>
            </div>
            <div class="hc-row">
              <span class="hc-label">Checked</span>
              <span class="hc-value" title="${_esc(r.checked_at || '')}">${_timeAgo(r.checked_at)}</span>
            </div>
            ${!r.ok && r.message ? `<div class="hc-error-msg">${_esc(r.message)}</div>` : ''}
          </div>
        </div>`;
    }).join('');

    container.innerHTML = `<div class="hc-grid">${cards}</div>`;

    // Attach handlers via event delegation (avoids XSS from inline onclick)
    container.querySelectorAll('button[data-db-id]').forEach(btn => {
      btn.addEventListener('click', () => HealthDashboard.refreshOne(btn.getAttribute('data-db-id')));
    });
  }

  function _esc(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  // ── Public API ───────────────────────────────────────────────────────────────

  async function refresh(container) {
    const btn = document.getElementById('hc-refresh-btn');
    if (btn) btn.disabled = true;
    try {
      _data = await fetchAll();
      render(container || document.getElementById('hc-cards'));
      updateLastRefreshed();
    } catch (e) {
      console.error('[HealthDashboard] refresh failed:', e);
    } finally {
      if (btn) btn.disabled = false;
    }
  }

  async function refreshOne(dbId) {
    try {
      const result = await fetchOne(dbId);
      _data[dbId] = result;
      render(document.getElementById('hc-cards'));
    } catch (e) {
      console.error('[HealthDashboard] refreshOne failed:', e);
    }
  }

  function updateLastRefreshed() {
    const el = document.getElementById('hc-last-refreshed');
    if (el) el.textContent = 'Last refreshed: ' + new Date().toLocaleTimeString();
  }

  function setSort(by) {
    _sortBy = by;
    render(document.getElementById('hc-cards'));
  }

  function setFilter(status) {
    _filterStatus = status;
    render(document.getElementById('hc-cards'));
  }

  function startAutoRefresh(seconds) {
    stopAutoRefresh();
    _autoInterval = seconds || _autoInterval;
    _autoTimer = setInterval(() => refresh(), _autoInterval * 1000);
  }

  function stopAutoRefresh() {
    if (_autoTimer) { clearInterval(_autoTimer); _autoTimer = null; }
  }

  return { refresh, refreshOne, setSort, setFilter, startAutoRefresh, stopAutoRefresh };
})();
