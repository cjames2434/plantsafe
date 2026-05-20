// Shared utilities: theme, saved-fields persistence, formatters.

(function () {
  const root = document.documentElement;
  const KEY_THEME = 'cropsentry.theme';
  const KEY_FIELDS = 'cropsentry.savedFields';
  const KEY_LOCATION = 'cropsentry.lastLocation';

  // ----- theme -----
  const stored = localStorage.getItem(KEY_THEME);
  const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
  root.dataset.theme = stored || (prefersDark ? 'dark' : 'light');

  document.addEventListener('click', (e) => {
    if (e.target.closest('#themeToggle')) {
      const next = root.dataset.theme === 'dark' ? 'light' : 'dark';
      root.dataset.theme = next;
      localStorage.setItem(KEY_THEME, next);
    }
  });

  // ----- saved fields API -----
  const PlantSafe = {
    getSaved() {
      try { return JSON.parse(localStorage.getItem(KEY_FIELDS) || '[]'); }
      catch { return []; }
    },
    saveField(field) {
      const list = PlantSafe.getSaved();
      const k = (f) => `${(+f.lat).toFixed(3)},${(+f.lon).toFixed(3)}|${f.crop}`;
      const filtered = list.filter(f => k(f) !== k(field));
      filtered.unshift({
        ...field,
        name: field.name || field.place,
        status: field.status || 'planned',
        notes: field.notes || '',
        savedAt: Date.now(),
      });
      localStorage.setItem(KEY_FIELDS, JSON.stringify(filtered.slice(0, 12)));
    },
    updateField(idx, updates) {
      const list = PlantSafe.getSaved();
      if (idx >= 0 && idx < list.length) {
        Object.assign(list[idx], updates);
        localStorage.setItem(KEY_FIELDS, JSON.stringify(list));
      }
    },
    removeField(idx) {
      const list = PlantSafe.getSaved();
      list.splice(idx, 1);
      localStorage.setItem(KEY_FIELDS, JSON.stringify(list));
    },
    clearSaved() { localStorage.removeItem(KEY_FIELDS); },
    fmtRelTime(ts) {
      const sec = Math.floor((Date.now() - ts) / 1000);
      if (sec < 60) return 'just now';
      if (sec < 3600) return `${Math.floor(sec / 60)}m ago`;
      if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`;
      return `${Math.floor(sec / 86400)}d ago`;
    },
  };

  PlantSafe.setLastLocation = function (place) {
    if (!place) return;
    localStorage.setItem(KEY_LOCATION, place);
    const el = document.getElementById('footerLocation');
    if (el) el.textContent = place.toUpperCase();
  };

  const lastLoc = localStorage.getItem(KEY_LOCATION);
  if (lastLoc) {
    const el = document.getElementById('footerLocation');
    if (el) el.textContent = lastLoc.toUpperCase();
  }

  window.PlantSafe = PlantSafe;
})();
