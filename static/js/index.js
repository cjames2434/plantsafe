// Dashboard: search form + saved fields list + seed-brand picker.

(function () {
  const form = document.getElementById('searchForm');
  const zipInput = document.getElementById('zipInput');
  const errBox = document.getElementById('searchError');
  const savedList = document.getElementById('savedFields');
  const clearBtn = document.querySelector('#savedFields')?.closest('.cs-saved-panel')?.querySelector('.cs-display + span');

  const submitBtn = form.querySelector('button[type="submit"]');
  const submitBtnHTML = submitBtn.innerHTML;

  // ----- crop chip radio sync -----------------------------------------------
  document.querySelectorAll('.cs-crop-chips input[type="radio"]').forEach(r => {
    r.addEventListener('change', () => {
      document.querySelectorAll('.cs-crop-chips .cs-chip').forEach(c => c.classList.remove('is-on'));
      if (r.checked && r.nextElementSibling) r.nextElementSibling.classList.add('is-on');
    });
  });

  // ----- tile drain toggle text ---------------------------------------------
  const tileCheck = document.getElementById('fieldTiledCheck');
  const tileStatus = document.getElementById('tileStatus');
  if (tileCheck && tileStatus) {
    tileCheck.addEventListener('change', () => {
      tileStatus.textContent = tileCheck.checked ? 'Drained' : 'Surface';
    });
  }

  // ----- animated counters (IntersectionObserver) ---------------------------
  function animateCounter(el) {
    const to = parseFloat(el.dataset.to);
    const prefix = el.dataset.prefix || '';
    const suffix = el.dataset.suffix || '';
    const decimals = parseInt(el.dataset.decimals || '0');
    const duration = 1800;
    const start = performance.now();
    function step(now) {
      const t = Math.min((now - start) / duration, 1);
      const eased = 1 - Math.pow(1 - t, 3);
      const val = eased * to;
      if (decimals > 0) {
        el.textContent = prefix + val.toFixed(decimals).replace(/\B(?=(\d{3})+(?!\d))/g, ',') + suffix;
      } else {
        el.textContent = prefix + Math.round(val).toLocaleString() + suffix;
      }
      if (t < 1) requestAnimationFrame(step);
    }
    requestAnimationFrame(step);
  }
  if ('IntersectionObserver' in window) {
    const obs = new IntersectionObserver((entries) => {
      entries.forEach(e => {
        if (e.isIntersecting) {
          animateCounter(e.target);
          obs.unobserve(e.target);
        }
      });
    }, { threshold: 0.3 });
    document.querySelectorAll('.cs-big-counter').forEach(el => obs.observe(el));
  } else {
    document.querySelectorAll('.cs-big-counter').forEach(el => animateCounter(el));
  }

  form.addEventListener('submit', (e) => {
    e.preventDefault();
    errBox.hidden = true;
    const zip = zipInput.value.trim();
    if (!zip) {
      errBox.textContent = 'Enter a ZIP, town, or coordinates.';
      errBox.hidden = false;
      return;
    }
    submitBtn.disabled = true;
    submitBtn.innerHTML = '<span class="spinner" style="width:16px;height:16px;border-width:2px" aria-hidden="true"></span> Evaluating…';
    const crop = document.querySelector('input[name="crop"]:checked').value;
    const params = new URLSearchParams({ zip, crop });
    const tillage = form.elements.tillage?.value;
    const residue = form.elements.residue?.value;
    if (tillage && tillage !== 'conventional') params.set('tillage', tillage);
    if (residue && residue !== 'low') params.set('residue', residue);
    if (form.elements.manure_recent?.checked) params.set('manure_recent', '1');
    if (form.elements.previous_grass?.checked) params.set('previous_grass', '1');
    if (form.elements.field_tiled?.checked) params.set('field_tiled', '1');
    const spa = form.elements.seeds_per_acre?.value?.trim();
    if (spa && parseInt(spa) >= 1000) params.set('seeds_per_acre', spa);
    const herb = form.elements.herbicide?.value.trim();
    if (herb) params.set('herbicide', herb);
    const seedBrand = document.getElementById('seedBrandInput').value;
    const seedCultivar = document.getElementById('seedCultivarInput').value;
    if (seedBrand && seedCultivar) {
      params.set('seed_brand', seedBrand);
      params.set('seed_cultivar', seedCultivar);
    }
    window.location.href = `/results?${params.toString()}`;
  });

  // ----- seed brand / cultivar picker ------------------------------------
  const seedBrandSelect = document.getElementById('seedBrandSelect');
  const seedList = document.getElementById('seedList');
  const seedListWrap = document.getElementById('seedListWrap');
  const seedSelected = document.getElementById('seedSelected');
  const seedClear = document.getElementById('seedClear');
  const seedBrandInput = document.getElementById('seedBrandInput');
  const seedCultivarInput = document.getElementById('seedCultivarInput');

  let seedCache = {};      // crop -> items[]
  let activeIdx = -1;

  function currentCrop() {
    return document.querySelector('input[name="crop"]:checked').value;
  }

  function loadSeeds(crop) {
    if (seedCache[crop]) return Promise.resolve(seedCache[crop]);
    return fetch(`/api/seeds?crop=${encodeURIComponent(crop)}`)
      .then((r) => r.ok ? r.json() : Promise.reject(new Error('seeds fetch failed')))
      .then((data) => {
        seedCache[crop] = data.items || [];
        return seedCache[crop];
      })
      .catch(() => []);
  }

  const allBrandOptions = Array.from(seedBrandSelect.options)
    .filter((o) => o.value)
    .map((o) => ({ value: o.value, text: o.textContent, crop: o.dataset.crop }));

  function filterBrandDropdown() {
    const crop = currentCrop();
    const placeholder = '<option value="">— Select a brand —</option>';
    const opts = allBrandOptions
      .filter((o) => o.crop === crop)
      .map((o) => `<option value="${escapeHTML(o.value)}">${escapeHTML(o.text)}</option>`);
    seedBrandSelect.innerHTML = placeholder + opts.join('');
  }

  function renderList(items) {
    activeIdx = -1;
    if (!items.length) {
      seedList.innerHTML = '<li class="seed-empty muted small">No cultivars available for this brand.</li>';
      return;
    }
    seedList.innerHTML = items.map((cv, i) => `
      <li class="seed-item" role="option" data-idx="${i}" tabindex="-1">
        <div class="seed-item-main">
          <span class="seed-cv">${escapeHTML(cv.id)}</span>
        </div>
        <div class="seed-item-sub muted small">${escapeHTML(cv.sub)}</div>
        ${cv.notes ? `<div class="seed-item-notes muted small">${escapeHTML(cv.notes)}</div>` : ''}
      </li>`).join('');
    seedList.dataset.filtered = JSON.stringify(items.map((cv) => [cv.brand, cv.id]));
  }

  function showCultivarsForBrand(brand) {
    if (!brand) {
      seedListWrap.hidden = true;
      seedList.innerHTML = '';
      return;
    }
    loadSeeds(currentCrop()).then((allItems) => {
      const items = allItems.filter((cv) => cv.brand === brand);
      renderList(items);
      seedListWrap.hidden = false;
    });
  }

  function pick(brand, cultivarId) {
    const crop = currentCrop();
    const cv = (seedCache[crop] || []).find((x) => x.brand === brand && x.id === cultivarId);
    if (!cv) return;
    seedBrandInput.value = brand;
    seedCultivarInput.value = cultivarId;
    seedSelected.hidden = false;
    seedSelected.innerHTML = `
      <div class="seed-pill">
        <strong>${escapeHTML(cv.brand)}</strong>
        <span>${escapeHTML(cv.id)}</span>
        <span class="seed-pill-sub muted small">${escapeHTML(cv.sub)}</span>
      </div>`;
    seedClear.hidden = false;
    seedListWrap.hidden = true;
    seedBrandSelect.value = '';
  }

  function clearPick() {
    seedBrandInput.value = '';
    seedCultivarInput.value = '';
    seedSelected.hidden = true;
    seedSelected.innerHTML = '';
    seedClear.hidden = true;
    seedBrandSelect.value = '';
    seedListWrap.hidden = true;
    seedList.innerHTML = '';
  }

  function setActive(i) {
    const lis = seedList.querySelectorAll('.seed-item');
    lis.forEach((el) => el.classList.remove('is-active'));
    if (i < 0 || i >= lis.length) { activeIdx = -1; return; }
    activeIdx = i;
    lis[i].classList.add('is-active');
    lis[i].scrollIntoView({ block: 'nearest' });
  }

  seedList.addEventListener('click', (ev) => {
    const li = ev.target.closest('.seed-item');
    if (!li) return;
    const filtered = JSON.parse(seedList.dataset.filtered || '[]');
    const pair = filtered[+li.dataset.idx];
    if (pair) pick(pair[0], pair[1]);
  });

  seedBrandSelect.addEventListener('change', () => {
    showCultivarsForBrand(seedBrandSelect.value);
  });

  seedClear.addEventListener('click', clearPick);

  document.querySelectorAll('input[name="crop"]').forEach((r) => {
    r.addEventListener('change', () => {
      clearPick();
      filterBrandDropdown();
    });
  });

  filterBrandDropdown();

  const savedCountEl = document.getElementById('savedCount');

  function renderSaved() {
    const list = window.PlantSafe ? window.PlantSafe.getSaved() : [];
    if (savedCountEl) savedCountEl.textContent = list.length + ' watching';
    if (!list.length) {
      savedList.innerHTML =
        '<li class="cs-mono" style="padding:14px 0;color:rgba(232,221,193,0.5);font-size:12px;letter-spacing:0.06em">No saved fields yet — evaluate a location and tap Save on the results page.</li>';
      return;
    }
    savedList.innerHTML = list.map((f, i) => {
      const params = new URLSearchParams({ lat: f.lat, lon: f.lon, crop: f.crop, place: f.place });
      const displayName = escapeHTML(f.name || f.place);
      const statusMap = { planned: ['rgba(232,221,193,0.45)', 'Watch'], planted: ['var(--amber)', 'Watch'], emerged: ['var(--green)', 'Plant'], harvested: ['var(--green)', 'Plant'] };
      const st = f.status || 'planned';
      const [stColor, stLabel] = statusMap[st] || ['rgba(232,221,193,0.45)', 'Watch'];
      return `
        <li style="display:grid;grid-template-columns:1fr auto auto;gap:16px;align-items:center;padding:14px 0;border-top:1px solid rgba(232,221,193,0.1)">
          <div>
            <a href="/results?${params}" style="font-size:14.5px;font-weight:500;color:var(--soil-paper);text-decoration:none">${displayName}</a>
            <div class="cs-mono" style="font-size:11px;color:rgba(232,221,193,0.5);margin-top:2px;letter-spacing:0.04em">${escapeHTML(capitalize(f.crop))}</div>
          </div>
          <span class="cs-display" style="font-size:26px;color:${stColor}">${escapeHTML(capitalize(st))}</span>
          <button data-idx="${i}" aria-label="Remove" style="background:none;border:none;color:rgba(232,221,193,0.4);cursor:pointer;font-size:16px">&times;</button>
        </li>`;
    }).join('');
  }

  savedList.addEventListener('click', (e) => {
    const btn = e.target.closest('button[data-idx]');
    if (!btn) return;
    if (window.PlantSafe) window.PlantSafe.removeField(+btn.dataset.idx);
    renderSaved();
  });

  function escapeHTML(s) { return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
  function capitalize(s) { return s.charAt(0).toUpperCase() + s.slice(1); }

  renderSaved();

  // ----- topbar weather (best-effort) ----------------------------------------
  const weatherEl = document.getElementById('topbarWeather');
  if (weatherEl) {
    const now = new Date();
    const time = now.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZoneName: 'short' });
    weatherEl.textContent = time;
  }

  // ----- subscribe form -----------------------------------------------------
  const subForm = document.getElementById('subscribeForm');
  const subMsg = document.getElementById('subscribeMsg');
  if (subForm) {
    subForm.addEventListener('submit', async (e) => {
      e.preventDefault();
      subMsg.hidden = true;
      const email = document.getElementById('subscribeEmail').value.trim();
      if (!email) return;
      const btn = subForm.querySelector('button');
      btn.disabled = true;
      btn.textContent = 'Subscribing…';
      try {
        const r = await fetch('/api/subscribe', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ email }),
        });
        const data = await r.json();
        if (data.ok) {
          subMsg.textContent = "You're signed up! We'll be in touch when alerts go live.";
          subMsg.className = 'subscribe-msg ok';
          subMsg.hidden = false;
          subForm.reset();
        } else {
          subMsg.textContent = data.error || 'Something went wrong.';
          subMsg.className = 'subscribe-msg err';
          subMsg.hidden = false;
        }
      } catch {
        subMsg.textContent = 'Network error — please try again.';
        subMsg.className = 'subscribe-msg err';
        subMsg.hidden = false;
      }
      btn.disabled = false;
      btn.textContent = 'Subscribe';
    });
  }
})();
