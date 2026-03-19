/**
 * notifications.js — Browser Notification Manager for DB Dump Manager
 *
 * Uses the native Web Notifications API to alert users when dumps complete,
 * even if the page is hidden or in a background tab.
 * ServiceWorker registration is attempted for background notification support.
 */

const NotificationManager = (() => {
  let _swRegistration = null;
  let _enabled = false;

  // ── ServiceWorker registration ──────────────────────────────────────────────

  async function registerServiceWorker() {
    if (!('serviceWorker' in navigator)) return null;
    try {
      const reg = await navigator.serviceWorker.register('/service-worker.js', { scope: '/' });
      _swRegistration = reg;
      return reg;
    } catch (e) {
      console.warn('[Notifications] ServiceWorker registration failed:', e);
      return null;
    }
  }

  // ── Permission ──────────────────────────────────────────────────────────────

  function isSupported() {
    return 'Notification' in window;
  }

  function getPermission() {
    return isSupported() ? Notification.permission : 'unsupported';
  }

  async function requestPermission() {
    if (!isSupported()) return 'unsupported';
    if (Notification.permission === 'granted') {
      _enabled = true;
      return 'granted';
    }
    const result = await Notification.requestPermission();
    _enabled = result === 'granted';
    return result;
  }

  function isEnabled() {
    return _enabled && isSupported() && Notification.permission === 'granted';
  }

  // ── Show notification ───────────────────────────────────────────────────────

  function _formatSize(bytes) {
    if (!bytes) return '';
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / 1048576).toFixed(2) + ' MB';
  }

  function show(data) {
    if (!isEnabled()) return;

    const isSuccess = data.status === 'done';
    const title = isSuccess
      ? `✅ Dump completed — ${data.db_name || 'database'}`
      : `❌ Dump failed — ${data.db_name || 'database'}`;

    const lines = [];
    if (data.filename)  lines.push(`File: ${data.filename}`);
    if (data.size)      lines.push(`Size: ${_formatSize(data.size)}`);
    if (data.duration_s != null) lines.push(`Duration: ${data.duration_s}s`);
    if (!isSuccess && data.message) lines.push(`Error: ${data.message}`);
    const body = lines.join('\n') || (isSuccess ? 'Dump saved successfully.' : 'Dump encountered an error.');

    const options = {
      body,
      icon:   '/static/icon.png',
      badge:  '/static/icon.png',
      tag:    `dump-${data.dump_id || Date.now()}`,
      data:   { url: '/#history', dump_id: data.dump_id },
      requireInteraction: false,
    };

    // Prefer ServiceWorker notification (works in background)
    if (_swRegistration && _swRegistration.showNotification) {
      _swRegistration.showNotification(title, options);
    } else {
      const n = new Notification(title, options);
      n.onclick = () => { window.focus(); n.close(); };
    }
  }

  // ── Handle Socket.IO dump_notification event ────────────────────────────────

  function handleDumpEvent(data) {
    // Only notify if page is hidden (user not looking at it)
    if (document.hidden || !document.hasFocus()) {
      show(data);
    }
  }

  // ── Persist preference ──────────────────────────────────────────────────────

  function loadPreference() {
    _enabled = localStorage.getItem('notifications_enabled') === 'true'
      && isSupported()
      && Notification.permission === 'granted';
  }

  function savePreference(val) {
    _enabled = val;
    localStorage.setItem('notifications_enabled', val ? 'true' : 'false');
  }

  // ── Init ────────────────────────────────────────────────────────────────────

  async function init() {
    loadPreference();
    if (_enabled) {
      await registerServiceWorker();
    }
  }

  return {
    init,
    isSupported,
    isEnabled,
    getPermission,
    requestPermission,
    registerServiceWorker,
    show,
    handleDumpEvent,
    savePreference,
    loadPreference,
  };
})();
