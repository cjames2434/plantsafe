// Push notification subscription management

(function() {
  'use strict';

  const ALERT_TYPES = {
    frost_alert: 'Frost/Freeze Warning',
    optimal_window: 'Optimal Planting Window',
    severe_weather: 'Severe Weather',
    crop_stress: 'Crop Stress (NDVI)',
    spray_window: 'Spray Window',
    soil_temp: 'Soil Temp Threshold',
  };

  let swRegistration = null;
  let isSubscribed = false;

  function registerServiceWorker() {
    if (!('serviceWorker' in navigator) || !('PushManager' in window)) return;

    navigator.serviceWorker.register('/static/sw.js')
      .then(function(reg) {
        swRegistration = reg;
        checkSubscription();
      })
      .catch(function() {});
  }

  function checkSubscription() {
    if (!swRegistration) return;
    swRegistration.pushManager.getSubscription()
      .then(function(sub) {
        isSubscribed = !!sub;
        updateUI();
      });
  }

  function updateUI() {
    const btn = document.getElementById('notifyBtn');
    if (!btn) return;
    if (!('PushManager' in window)) {
      btn.hidden = true;
      return;
    }
    btn.hidden = false;
    var label = btn.querySelector('span');
    if (label) {
      label.textContent = isSubscribed ? 'Alerts On' : 'Alerts';
    } else {
      btn.textContent = isSubscribed ? 'Alerts On' : 'Alerts';
    }
    btn.classList.toggle('active', isSubscribed);
  }

  function subscribe(lat, lon, place, crop, alerts) {
    if (!swRegistration) return Promise.reject(new Error('No service worker'));

    return swRegistration.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: urlBase64ToUint8Array(getVapidPublicKey()),
    }).then(function(subscription) {
      const sub = subscription.toJSON();
      return fetch('/api/push/subscribe', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          endpoint: sub.endpoint,
          keys: sub.keys,
          lat: lat,
          lon: lon,
          place: place,
          crop: crop,
          alerts: alerts || Object.keys(ALERT_TYPES),
        }),
      });
    }).then(function(r) { return r.json(); })
    .then(function(data) {
      if (data.ok) {
        isSubscribed = true;
        updateUI();
      }
      return data;
    });
  }

  function unsubscribe() {
    if (!swRegistration) return Promise.resolve();
    return swRegistration.pushManager.getSubscription()
      .then(function(sub) {
        if (sub) return sub.unsubscribe();
      }).then(function() {
        isSubscribed = false;
        updateUI();
      });
  }

  function getVapidPublicKey() {
    return document.querySelector('meta[name="vapid-public-key"]')?.content || '';
  }

  function urlBase64ToUint8Array(base64String) {
    const padding = '='.repeat((4 - base64String.length % 4) % 4);
    const base64 = (base64String + padding).replace(/-/g, '+').replace(/_/g, '/');
    const rawData = window.atob(base64);
    const outputArray = new Uint8Array(rawData.length);
    for (let i = 0; i < rawData.length; ++i) {
      outputArray[i] = rawData.charCodeAt(i);
    }
    return outputArray;
  }

  function escapeHTML(s) { return String(s).replace(/[&<>"']/g, function(c) { return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]; }); }

  function showAlertPreferences(lat, lon, place, crop) {
    const existing = document.getElementById('alertPrefsModal');
    if (existing) existing.remove();

    const modal = document.createElement('div');
    modal.id = 'alertPrefsModal';
    modal.className = 'alert-prefs-modal';
    modal.innerHTML = `
      <div class="alert-prefs-backdrop"></div>
      <div class="alert-prefs-dialog">
        <h3>Field Alerts</h3>
        <p class="muted small">Get notified when conditions change at <strong>${escapeHTML(place || 'this location')}</strong>.</p>
        <div class="alert-prefs-list">
          ${Object.entries(ALERT_TYPES).map(([key, label]) => `
            <label class="alert-pref-item">
              <input type="checkbox" name="alert" value="${key}" checked>
              <span>${label}</span>
            </label>
          `).join('')}
        </div>
        <div class="alert-prefs-actions">
          <button type="button" class="btn primary" id="alertSubscribeBtn">Enable Alerts</button>
          <button type="button" class="btn ghost" id="alertCancelBtn">Cancel</button>
        </div>
        ${!('PushManager' in window) ? '<p class="alert-prefs-unsupported muted small">Push notifications are not supported in this browser.</p>' : ''}
      </div>
    `;
    document.body.appendChild(modal);

    modal.querySelector('.alert-prefs-backdrop').addEventListener('click', function() { modal.remove(); });
    modal.querySelector('#alertCancelBtn').addEventListener('click', function() { modal.remove(); });
    modal.querySelector('#alertSubscribeBtn').addEventListener('click', function() {
      const checked = Array.from(modal.querySelectorAll('input[name="alert"]:checked'))
        .map(function(cb) { return cb.value; });
      if (!checked.length) { modal.remove(); return; }

      Notification.requestPermission().then(function(perm) {
        if (perm !== 'granted') {
          modal.remove();
          return;
        }
        subscribe(lat, lon, place, crop, checked).then(function() {
          modal.remove();
        }).catch(function() { modal.remove(); });
      });
    });
  }

  function checkAlerts(lat, lon, crop) {
    const qs = new URLSearchParams({ lat, lon, crop });
    return fetch(`/api/push/check?${qs}`)
      .then(function(r) { return r.ok ? r.json() : null; })
      .then(function(data) { return data ? data.alerts : []; });
  }

  // Expose API
  window.PlantSafeNotifications = {
    register: registerServiceWorker,
    subscribe: subscribe,
    unsubscribe: unsubscribe,
    showPreferences: showAlertPreferences,
    checkAlerts: checkAlerts,
    isSubscribed: function() { return isSubscribed; },
    ALERT_TYPES: ALERT_TYPES,
  };

  // Auto-register on page load
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', registerServiceWorker);
  } else {
    registerServiceWorker();
  }
})();
