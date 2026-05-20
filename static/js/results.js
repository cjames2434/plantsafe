// Results page: fetch /api/evaluate, render verdict + risks + chart + daily strip.

(function () {
  const params = new URLSearchParams(window.location.search);
  const zip  = params.get('zip');
  const lat  = params.get('lat');
  const lon  = params.get('lon');
  let crop = params.get('crop') || 'corn';

  const loading = document.getElementById('loading');
  const errBox = document.getElementById('errorBox');
  const errMsg = document.getElementById('errorMsg');
  const report = document.getElementById('report');
  const cropToggle = document.getElementById('cropToggle');

  const apiQs = new URLSearchParams({ crop });
  if (lat && lon) { apiQs.set('lat', lat); apiQs.set('lon', lon); }
  else if (zip)   { apiQs.set('zip', zip); }
  else { return showError('Missing zip or coordinates in URL.'); }
  syncCropToggle();
  ['tillage', 'residue', 'manure_recent', 'previous_grass', 'herbicide',
   'field_tiled', 'seeds_per_acre', 'seed_brand', 'seed_cultivar'].forEach((k) => {
    const v = params.get(k);
    if (v) apiQs.set(k, v);
  });

  let chartInstance = null;
  let latest = null;  // most recent /api/evaluate response
  let navObserver = null;
  load();

  function load() {
    errBox.hidden = true;
    report.hidden = true;
    loading.hidden = false;
    // Cache-bust so the browser doesn't serve a stale Open-Meteo response.
    fetch(`/api/evaluate?${apiQs}&_=${Date.now()}`)
      .then(async (r) => {
        if (!r.ok) {
          const body = await r.json().catch(() => ({}));
          throw new Error(body.detail || body.error || `Request failed (${r.status})`);
        }
        return r.json();
      })
      .then(render)
      .catch((e) => showError(e.message));
  }

  function showError(msg) {
    loading.hidden = true;
    errMsg.textContent = msg;
    errBox.hidden = false;
  }

  function formatUpdatedAt(d) {
    return d.toLocaleString(undefined, {
      weekday: 'short', month: 'short', day: 'numeric',
      hour: 'numeric', minute: '2-digit',
    });
  }

  function worstLevel(risks) {
    const order = { low: 0, moderate: 1, high: 2 };
    return risks.reduce((acc, r) => order[r.level] > order[acc] ? r.level : acc, 'low');
  }

  // Open-Meteo daily times are date-only ("YYYY-MM-DD"). new Date() parses those as
  // UTC midnight, which shifts back a day in any negative-UTC timezone — pin to local.
  function parseLocalDate(iso) {
    const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(iso);
    if (!m) return new Date(iso);
    return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  }

  function render(data) {
    latest = data;
    loading.hidden = true;
    report.hidden = false;

    if (data.outside_us) {
      document.getElementById('placeName').textContent = data.location.place;
      const verdict = document.getElementById('verdict');
      verdict.dataset.level = 'moderate';
      document.getElementById('verdictText').textContent = 'TBD';
      document.getElementById('verdictDetail').textContent = data.verdict_detail;
      document.getElementById('riskList').innerHTML = `
        <li class="risk-card" data-level="moderate" style="text-align:center;padding:24px;">
          <div class="risk-body">
            <h3>International Coverage Unavailable</h3>
            <p class="headline">We are currently only calculating survival for locations within the United States of America. International coverage is coming soon.</p>
          </div>
        </li>`;
      return;
    }

    document.getElementById('placeName').textContent = data.location.place;
    if (window.PlantSafe) window.PlantSafe.setLastLocation(data.location.place);
    syncCropToggle();

    const verdict = document.getElementById('verdict');
    const level = worstLevel(data.risks);
    verdict.dataset.level = level;
    document.getElementById('verdictText').textContent = data.recommendation;
    document.getElementById('verdictDetail').textContent = data.verdict_detail;

    // Crop Sentry: populate aerial strip + verdict badge + survival gauge
    var aerialPlace = document.getElementById('aerialPlace');
    if (aerialPlace) {
      aerialPlace.innerHTML = data.location.place.replace(/, /, ' · <em style="color:var(--amber-2)">') + '</em>';
    }
    var aerialMeta = document.getElementById('aerialMeta');
    if (aerialMeta) {
      var cropLabel = (data.crop && data.crop.label) || '';
      aerialMeta.textContent = '§ FIELD · ' + cropLabel.toUpperCase();
    }
    var aerialCoords = document.getElementById('aerialCoords');
    if (aerialCoords && data.location) {
      aerialCoords.innerHTML = data.location.lat.toFixed(4) + '°N · ' + Math.abs(data.location.lon).toFixed(4) + '°W';
    }
    var verdictBadge = document.getElementById('verdictBadge');
    if (verdictBadge) {
      var badgeColors = { low: 'var(--green)', moderate: 'var(--amber)', high: 'var(--red)' };
      var badgeWords = { low: 'OPTIMAL', moderate: 'PROCEED WITH CAUTION', high: 'DO NOT PLANT' };
      verdictBadge.style.background = badgeColors[level] || 'var(--amber)';
      verdictBadge.style.color = 'var(--paper)';
      verdictBadge.textContent = '◆ ' + (badgeWords[level] || data.recommendation);
    }
    var survivalNum = document.getElementById('survivalNum');
    var survivalFill = document.getElementById('survivalFill');
    var survivalPct = data.survival && data.survival.point_pct != null ? Math.round(data.survival.point_pct) : null;
    if (survivalNum && survivalPct != null) {
      survivalNum.textContent = survivalPct;
      var gaugeColor = survivalPct >= 70 ? 'var(--green)' : survivalPct >= 40 ? 'var(--amber)' : 'var(--red)';
      survivalNum.style.color = gaugeColor;
      if (survivalFill) {
        survivalFill.style.width = survivalPct + '%';
        survivalFill.style.background = gaugeColor;
      }
      var survivalPctEl = document.querySelector('.cs-survival-pct');
      if (survivalPctEl) survivalPctEl.style.color = gaugeColor;
    }

    // Commercial farming region disclaimer
    const cfDisclaimer = document.getElementById('commercialFarmingDisclaimer');
    if (cfDisclaimer) {
      const cf = data.commercial_farming;
      if (cf && cf.disclaimer) {
        cfDisclaimer.hidden = false;
        cfDisclaimer.textContent = cf.disclaimer;
      } else {
        cfDisclaimer.hidden = true;
      }
    }

    renderVerdictConfidence(data);

    document.getElementById('kpiSoil').textContent = `${data.summary.min_soil_temp_f.toFixed(1)}°F`;
    document.getElementById('kpiPrecip').textContent = `${data.summary.total_precip_in_48h.toFixed(2)}"`;
    document.getElementById('kpiUv').textContent = data.summary.max_uv_48h.toFixed(1);

    var stickyBar = document.getElementById('verdictSticky');
    if (stickyBar) {
      stickyBar.dataset.level = level;
      var vsV = document.getElementById('vsVerdict');
      if (vsV) vsV.textContent = data.recommendation;
      var vsP = document.getElementById('vsPlace');
      if (vsP) vsP.textContent = data.location.place;
      var vsS = document.getElementById('vsSoil');
      if (vsS) vsS.textContent = data.summary.min_soil_temp_f.toFixed(1) + '°F';
      var vsPr = document.getElementById('vsPrecip');
      if (vsPr) vsPr.textContent = data.summary.total_precip_in_48h.toFixed(2) + '"';
      var verdictEl = document.getElementById('verdict');
      var stickyObs = new IntersectionObserver(function (entries) {
        stickyBar.classList.toggle('is-visible', !entries[0].isIntersecting);
      }, { threshold: 0 });
      stickyObs.observe(verdictEl);
    }

    document.getElementById('mapLink').href =
      `/map?lat=${data.location.lat}&lon=${data.location.lon}&crop=${data.crop.key}`;

    const risks = ensureMaggotFromScm(data.risks || [], data.scm_forecast);
    document.getElementById('riskList').innerHTML = risks.map(riskCard).join('');
    wireRiskCards();

    renderDepth(data.planting_depth, data.crop);
    renderSeed(data.seed, data.crop);
    renderDataSources(data);

    renderTacticalCalendar(data.plant_days, data.spray_windows);
    renderSprayWindows(data.spray_windows);
    wireIcalExport(data);

    document.getElementById('plantDays').innerHTML = renderPlantDays(data.plant_days, data.best_days, data.crop);
    document.getElementById('dailyStrip').innerHTML = renderDaily(data.daily);
    renderHistory(data.history, data.climatology, data.daily);

    setupExtendedToggles(data);
    buildResultsNav();

    drawChart(data.hourly);
    fetchAndRenderNDVI();
    fetchAndRenderAlerts();
    fetchAndRenderFertility();
    fetchAndRenderRotation();
    fetchAndRenderYield();
    fetchAndRenderProfit();
    fetchAndRenderCommunity();
    fetchAndRenderValidation();
    wireNotifyButton(data);

    document.getElementById('updatedAt').textContent =
      `Updated ${formatUpdatedAt(new Date())}`;
  }

  function buildResultsNav() {
    const nav = document.getElementById('resultsNav');
    if (!nav) return;
    const sections = Array.from(document.querySelectorAll('[data-nav-label]'))
      .filter((s) => s.id && !s.hidden);
    if (sections.length < 2) {
      nav.hidden = true;
      nav.innerHTML = '';
      if (navObserver) { navObserver.disconnect(); navObserver = null; }
      return;
    }
    nav.innerHTML = sections.map((s) =>
      `<a href="#${s.id}" data-target="${s.id}">${escapeHTML(s.dataset.navLabel)}</a>`
    ).join('');
    nav.hidden = false;

    // Smooth-scroll with offset so the sticky nav doesn't cover the heading.
    const headerOffset = 72;
    nav.querySelectorAll('a').forEach((a) => {
      a.addEventListener('click', (ev) => {
        ev.preventDefault();
        const tgt = document.getElementById(a.dataset.target);
        if (!tgt) return;
        const y = tgt.getBoundingClientRect().top + window.scrollY - headerOffset;
        window.scrollTo({ top: y, behavior: 'smooth' });
      });
    });

    // Highlight whichever section currently dominates the viewport.
    if (navObserver) navObserver.disconnect();
    if (typeof IntersectionObserver === 'undefined') return;
    navObserver = new IntersectionObserver((entries) => {
      entries.forEach((en) => {
        if (!en.isIntersecting) return;
        const link = nav.querySelector(`a[data-target="${en.target.id}"]`);
        if (!link) return;
        nav.querySelectorAll('a').forEach((a) => a.classList.remove('is-active'));
        link.classList.add('is-active');
      });
    }, { rootMargin: '-25% 0px -60% 0px', threshold: 0 });
    sections.forEach((s) => navObserver.observe(s));
  }

  document.getElementById('saveBtn').addEventListener('click', () => {
    if (!latest) return;
    const btn = document.getElementById('saveBtn');
    let wrap = document.getElementById('saveDetailWrap');
    if (wrap) { wrap.hidden = !wrap.hidden; return; }
    wrap = document.createElement('div');
    wrap.id = 'saveDetailWrap';
    wrap.className = 'save-detail';
    wrap.innerHTML = `
      <label class="field compact"><span class="field-label">Field name</span>
        <input id="saveFieldName" type="text" placeholder="${escapeHTML(latest.location.place)}" maxlength="60" /></label>
      <label class="field compact"><span class="field-label">Status</span>
        <select id="saveFieldStatus">
          <option value="planned">Planned</option>
          <option value="planted">Planted</option>
          <option value="emerged">Emerged</option>
          <option value="harvested">Harvested</option>
        </select></label>
      <label class="field compact"><span class="field-label">Notes</span>
        <textarea id="saveFieldNotes" rows="2" maxlength="200" placeholder="Optional notes…"></textarea></label>
      <button class="btn primary small" id="confirmSave" type="button">Save field</button>
    `;
    btn.parentElement.after(wrap);
    document.getElementById('confirmSave').addEventListener('click', () => {
      const name = document.getElementById('saveFieldName').value.trim() || latest.location.place;
      const status = document.getElementById('saveFieldStatus').value;
      const notes = document.getElementById('saveFieldNotes').value.trim();
      window.PlantSafe.saveField({
        lat: latest.location.lat, lon: latest.location.lon,
        place: latest.location.place, crop: latest.crop.key,
        name, status, notes,
      });
      wrap.remove();
      btn.textContent = 'Saved';
      btn.disabled = true;
    });
  });

  // ----- share link ---------------------------------------------------------
  document.getElementById('shareBtn').addEventListener('click', () => {
    const url = window.location.href;
    const btn = document.getElementById('shareBtn');
    const label = btn.querySelector('span');
    if (navigator.share) {
      navigator.share({ title: 'Crop Sentry Assessment', url }).catch(() => {});
    } else if (navigator.clipboard) {
      navigator.clipboard.writeText(url).then(() => {
        label.textContent = 'Copied!';
        setTimeout(() => { label.textContent = 'Share'; }, 2000);
      });
    }
  });

  // ----- CSV export ---------------------------------------------------------
  document.getElementById('exportCsvBtn').addEventListener('click', () => {
    if (!latest) return;
    const rows = [['Factor', 'Level', 'Headline', 'Metric']];
    (latest.risks || []).forEach(r => {
      rows.push([r.name, r.level, r.headline, r.metric || '']);
    });
    rows.push([]);
    rows.push(['Location', latest.location.place]);
    rows.push(['Crop', latest.crop.label]);
    rows.push(['Recommendation', latest.recommendation]);
    rows.push(['Min Soil Temp 48h (F)', latest.summary.min_soil_temp_f.toFixed(1)]);
    rows.push(['Total Precip 48h (in)', latest.summary.total_precip_in_48h.toFixed(2)]);
    rows.push(['Peak UV 48h', latest.summary.max_uv_48h.toFixed(1)]);
    if (latest.survival && latest.survival.point_pct != null) {
      rows.push(['Survival %', latest.survival.point_pct]);
    }
    if (latest.plant_days) {
      rows.push([]);
      rows.push(['Date', 'Day Score', 'Verdict']);
      latest.plant_days.forEach(d => {
        rows.push([d.date, d.score != null ? d.score : '', d.verdict || '']);
      });
    }
    const csv = rows.map(r => r.map(c => '"' + String(c).replace(/"/g, '""') + '"').join(',')).join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'cropsentry-' + (latest.location.place || 'report').replace(/[^a-zA-Z0-9]/g, '-') + '.csv';
    a.click();
    URL.revokeObjectURL(a.href);
  });

  // ----- PDF export ---------------------------------------------------------
  const pdfBtn = document.getElementById('exportPdfBtn');
  if (pdfBtn) {
    pdfBtn.addEventListener('click', () => {
      if (!latest) return;
      const pdfParams = new URLSearchParams(apiQs);
      pdfBtn.disabled = true;
      const label = pdfBtn.querySelector('span');
      if (label) label.textContent = 'Generating…';
      fetch(`/api/export/pdf?${pdfParams}`)
        .then(r => {
          if (!r.ok) throw new Error('PDF generation failed');
          return r.blob();
        })
        .then(blob => {
          const a = document.createElement('a');
          a.href = URL.createObjectURL(blob);
          a.download = 'cropsentry-' + (latest.location.place || 'report').replace(/[^a-zA-Z0-9]/g, '-') + '.pdf';
          a.click();
          URL.revokeObjectURL(a.href);
        })
        .catch(() => {
          alert('Could not generate PDF. Please try again.');
        })
        .finally(() => {
          pdfBtn.disabled = false;
          if (label) label.textContent = 'Export PDF';
        });
    });
  }

  function syncCropToggle() {
    if (!cropToggle) return;
    cropToggle.querySelectorAll('.crop-toggle-btn').forEach((btn) => {
      btn.classList.toggle('is-active', btn.dataset.crop === crop);
    });
  }

  if (cropToggle) {
    cropToggle.addEventListener('click', (ev) => {
      const btn = ev.target.closest('.crop-toggle-btn');
      if (!btn || btn.dataset.crop === crop) return;
      crop = btn.dataset.crop;
      apiQs.set('crop', crop);
      // A cultivar belongs to one crop; clear the picker carry-over so the
      // backend doesn't try to look up a corn hybrid in the soybean catalog.
      apiQs.delete('seed_brand');
      apiQs.delete('seed_cultivar');
      const url = new URL(window.location.href);
      url.searchParams.set('crop', crop);
      url.searchParams.delete('seed_brand');
      url.searchParams.delete('seed_cultivar');
      window.history.replaceState(null, '', url);
      syncCropToggle();
      if (window._seedPickerFilterReset) window._seedPickerFilterReset();
      else load();
    });
  }

  // ----- tile-drainage toggle -----------------------------------------------
  const tileCheck = document.getElementById('fieldTiledCheck');
  if (tileCheck) {
    tileCheck.checked = params.get('field_tiled') === '1';
    tileCheck.addEventListener('change', () => {
      const url = new URL(window.location.href);
      if (tileCheck.checked) {
        apiQs.set('field_tiled', '1');
        url.searchParams.set('field_tiled', '1');
      } else {
        apiQs.delete('field_tiled');
        url.searchParams.delete('field_tiled');
      }
      window.history.replaceState(null, '', url);
      load();
    });
  }

  // ----- seeds per acre (population) input -----------------------------------
  const popInput = document.getElementById('seedsPerAcre');
  if (popInput) {
    const existingPop = params.get('seeds_per_acre');
    if (existingPop) popInput.value = existingPop;
    let popDebounce = null;
    popInput.addEventListener('change', () => {
      clearTimeout(popDebounce);
      popDebounce = setTimeout(() => {
        const url = new URL(window.location.href);
        const val = popInput.value.trim();
        if (val && parseInt(val) >= 1000) {
          apiQs.set('seeds_per_acre', val);
          url.searchParams.set('seeds_per_acre', val);
        } else {
          apiQs.delete('seeds_per_acre');
          url.searchParams.delete('seeds_per_acre');
        }
        window.history.replaceState(null, '', url);
        load();
      }, 300);
    });
  }

  document.getElementById('refreshBtn').addEventListener('click', () => {
    const btn = document.getElementById('refreshBtn');
    btn.disabled = true;
    btn.querySelector('span').textContent = 'Refreshing…';
    load();
    // Re-enable shortly after; load() flips the loading panel itself.
    setTimeout(() => {
      btn.disabled = false;
      btn.querySelector('span').textContent = 'Refresh';
    }, 800);
  });

  // ----- seed brand / cultivar picker (results page) -----------------------
  (function initSeedPicker() {
    const seedBrandSelect = document.getElementById('seedBrandSelect');
    const seedList = document.getElementById('seedList');
    const seedListWrap = document.getElementById('seedListWrap');
    const seedSelected = document.getElementById('seedSelected');
    const seedClear = document.getElementById('seedClear');
    if (!seedBrandSelect) return;

    let seedCache = {};
    let activeIdx = -1;

    const allBrandOptions = Array.from(seedBrandSelect.options)
      .filter((o) => o.value)
      .map((o) => ({ value: o.value, text: o.textContent, crop: o.dataset.crop }));

    function filterBrandDropdown() {
      const placeholder = '<option value="">— Select a brand —</option>';
      const opts = allBrandOptions
        .filter((o) => o.crop === crop)
        .map((o) => `<option value="${escapeHTML(o.value)}">${escapeHTML(o.text)}</option>`);
      seedBrandSelect.innerHTML = placeholder + opts.join('');
    }

    function loadSeeds(c) {
      if (seedCache[c]) return Promise.resolve(seedCache[c]);
      return fetch(`/api/seeds?crop=${encodeURIComponent(c)}`)
        .then((r) => r.ok ? r.json() : Promise.reject(new Error('seeds fetch failed')))
        .then((data) => { seedCache[c] = data.items || []; return seedCache[c]; })
        .catch(() => []);
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
      if (!brand) { seedListWrap.hidden = true; seedList.innerHTML = ''; return; }
      loadSeeds(crop).then((allItems) => {
        renderList(allItems.filter((cv) => cv.brand === brand));
        seedListWrap.hidden = false;
      });
    }

    function pick(brand, cultivarId) {
      const cv = (seedCache[crop] || []).find((x) => x.brand === brand && x.id === cultivarId);
      if (!cv) return;
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
      apiQs.set('seed_brand', brand);
      apiQs.set('seed_cultivar', cultivarId);
      const url = new URL(window.location.href);
      url.searchParams.set('seed_brand', brand);
      url.searchParams.set('seed_cultivar', cultivarId);
      window.history.replaceState(null, '', url);
      load();
    }

    function clearPick() {
      seedSelected.hidden = true;
      seedSelected.innerHTML = '';
      seedClear.hidden = true;
      seedBrandSelect.value = '';
      seedListWrap.hidden = true;
      seedList.innerHTML = '';
      apiQs.delete('seed_brand');
      apiQs.delete('seed_cultivar');
      const url = new URL(window.location.href);
      url.searchParams.delete('seed_brand');
      url.searchParams.delete('seed_cultivar');
      window.history.replaceState(null, '', url);
      load();
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

    filterBrandDropdown();

    // Pre-populate if seed params already in URL
    const initBrand = params.get('seed_brand');
    const initCultivar = params.get('seed_cultivar');
    if (initBrand && initCultivar) {
      loadSeeds(crop).then(() => {
        const cv = (seedCache[crop] || []).find((x) => x.brand === initBrand && x.id === initCultivar);
        if (cv) {
          seedSelected.hidden = false;
          seedSelected.innerHTML = `
            <div class="seed-pill">
              <strong>${escapeHTML(cv.brand)}</strong>
              <span>${escapeHTML(cv.id)}</span>
              <span class="seed-pill-sub muted small">${escapeHTML(cv.sub)}</span>
            </div>`;
          seedClear.hidden = false;
        }
      });
    }

    // Expose filter reset for crop toggle — UI-only, caller handles reload
    window._seedPickerFilterReset = function () {
      seedSelected.hidden = true;
      seedSelected.innerHTML = '';
      seedClear.hidden = true;
      seedBrandSelect.value = '';
      seedListWrap.hidden = true;
      seedList.innerHTML = '';
      filterBrandDropdown();
      load();
    };
  })();

  function riskCard(r) {
    const icons = {
      chilling: '<svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2v20M4.93 4.93l14.14 14.14M2 12h20M4.93 19.07l14.14-14.14"/></svg>',
      flooding: '<svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 16c2 0 2-2 4-2s2 2 4 2 2-2 4-2 2 2 4 2M3 20c2 0 2-2 4-2s2 2 4 2 2-2 4-2 2 2 4 2M12 4l3 5h-6l3-5Z"/></svg>',
    };
    const icon = icons[r.key] || '<svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="12" cy="12" r="9"/><path d="M12 8v4M12 16h.01"/></svg>';
    const profile = RISK_PROFILES[r.key];
    const expandable = !!profile;
    const infoBtn = `
      <button type="button" class="risk-info-btn"
              aria-label="How is ${escapeAttr(r.name)} scored?"
              aria-expanded="false"
              data-key="${escapeAttr(r.key)}">
        <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="M12 8v.01M11 12h1v4h1"/></svg>
      </button>`;
    return `
      <li class="risk-card${expandable ? ' is-expandable' : ''}" data-level="${r.level}" data-key="${escapeAttr(r.key)}">
        ${infoBtn}
        <button type="button" class="risk-summary" aria-expanded="false"${expandable ? '' : ' disabled'}>
          <span class="risk-icon">${icon}</span>
          <div class="risk-body">
            <h3>${escapeHTML(r.name)}</h3>
            <p class="headline">${escapeHTML(r.headline)}</p>
          </div>
          <span class="risk-metric">${escapeHTML(r.metric || '')}</span>
          ${expandable ? '<span class="risk-chevron" aria-hidden="true"><svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"/></svg></span>' : ''}
        </button>
        <div class="risk-detail" hidden>
          <p class="risk-detail-why">${escapeHTML(r.detail)}</p>
          <div class="risk-windows" data-pending="1"></div>
        </div>
      </li>`;
  }

  // ----- methodology popover (lazy-loaded) -------------------------------
  let methodologyPromise = null;
  function loadMethodology() {
    if (!methodologyPromise) {
      methodologyPromise = fetch('/api/methodology')
        .then((r) => r.ok ? r.json() : Promise.reject(new Error('methodology fetch failed')))
        .catch((err) => { methodologyPromise = null; throw err; });
    }
    return methodologyPromise;
  }

  function closeMethodologyPopover() {
    const open = document.querySelector('.risk-methodology-popover');
    if (open) open.remove();
    document.querySelectorAll('.risk-info-btn[aria-expanded="true"]').forEach((b) => {
      b.setAttribute('aria-expanded', 'false');
    });
  }

  function positionPopover(pop, btn) {
    const rect = btn.getBoundingClientRect();
    const popW = Math.min(360, window.innerWidth - 24);
    pop.style.width = popW + 'px';
    // Anchor to the right edge of the button, place below by default.
    let left = rect.right + window.scrollX - popW;
    if (left < 12) left = 12;
    if (left + popW > window.scrollX + window.innerWidth - 12) {
      left = window.scrollX + window.innerWidth - popW - 12;
    }
    let top = rect.bottom + window.scrollY + 8;
    pop.style.left = left + 'px';
    pop.style.top = top + 'px';
    // After mount, flip above the button if the popover would overflow viewport bottom.
    requestAnimationFrame(() => {
      const popRect = pop.getBoundingClientRect();
      if (popRect.bottom > window.innerHeight - 12 && rect.top > popRect.height + 12) {
        pop.style.top = (rect.top + window.scrollY - popRect.height - 8) + 'px';
      }
    });
  }

  function openMethodologyPopover(btn) {
    closeMethodologyPopover();
    const key = btn.dataset.key;
    const pop = document.createElement('div');
    pop.className = 'risk-methodology-popover';
    pop.setAttribute('role', 'dialog');
    pop.setAttribute('aria-label', 'How this risk is scored');
    pop.innerHTML = '<p class="muted small" style="margin:0">Loading…</p>';
    document.body.appendChild(pop);
    positionPopover(pop, btn);
    btn.setAttribute('aria-expanded', 'true');

    loadMethodology().then((data) => {
      const entry = (data.risks || []).find((e) => e.key === key);
      if (!entry) {
        pop.innerHTML = '<p class="muted small" style="margin:0">No methodology entry for this risk.</p>';
        return;
      }
      const rows = (entry.thresholds || []).map(([level, criteria]) => `
        <tr>
          <td><span class="meth-pill" data-level="${escapeAttr(level)}">${escapeHTML(level)}</span></td>
          <td>${escapeHTML(criteria)}</td>
        </tr>`).join('');
      pop.innerHTML = `
        <header class="rmp-head">
          <h4>${escapeHTML(entry.name)} — how it's scored</h4>
          <button type="button" class="rmp-close" aria-label="Close">
            <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M6 6l12 12M6 18L18 6"/></svg>
          </button>
        </header>
        <p class="rmp-summary">${escapeHTML(entry.summary)}</p>
        <table class="rmp-table"><tbody>${rows}</tbody></table>
        <a class="rmp-link" href="/methodology#${escapeAttr(entry.key)}">Read the full methodology &rarr;</a>`;
      pop.querySelector('.rmp-close').addEventListener('click', (ev) => {
        ev.stopPropagation();
        closeMethodologyPopover();
      });
    }).catch(() => {
      pop.innerHTML = '<p class="muted small" style="margin:0">Couldn\'t load methodology — try the full page.</p>';
    });
  }

  // Single delegated handler for all info buttons + outside-click + Escape.
  document.addEventListener('click', (ev) => {
    const btn = ev.target.closest('.risk-info-btn');
    if (btn) {
      ev.stopPropagation();
      const isOpen = btn.getAttribute('aria-expanded') === 'true';
      if (isOpen) closeMethodologyPopover();
      else openMethodologyPopover(btn);
      return;
    }
    if (!ev.target.closest('.risk-methodology-popover')) closeMethodologyPopover();
  });
  document.addEventListener('keydown', (ev) => {
    if (ev.key === 'Escape') closeMethodologyPopover();
  });

  function wireRiskCards() {
    document.querySelectorAll('.risk-card.is-expandable').forEach((card) => {
      const summary = card.querySelector('.risk-summary');
      const detail = card.querySelector('.risk-detail');
      summary.addEventListener('click', () => {
        const open = card.classList.toggle('is-open');
        summary.setAttribute('aria-expanded', open ? 'true' : 'false');
        detail.hidden = !open;
        if (open && !card.dataset.detailLoaded) {
          loadRiskDetail(card);
          card.dataset.detailLoaded = '1';
        }
      });
    });
  }

  function loadRiskDetail(card) {
    const placeholder = card.querySelector('.risk-windows');
    if (!placeholder) return;
    const scm = latest && latest.scm_forecast;
    if (card.dataset.key === 'maggot' && scm && scm.available && scm.days && scm.days.length) {
      placeholder.outerHTML = renderScmDetail(scm);
      wireScmExtendedToggle(card);
    } else {
      placeholder.innerHTML = renderRiskWindows(card.dataset.key);
    }
  }

  // If the evaluator surfaced a seedcorn-maggot survival forecast but didn't
  // raise an explicit "maggot" risk card, synthesize one so the user still has
  // an entry point to the detailed view.
  function ensureMaggotFromScm(risks, scm) {
    if (!scm || !scm.available || !scm.days || !scm.days.length) return risks;
    if (risks.some((r) => r.key === 'maggot')) return risks;
    const days = scm.days;
    const survPcts = days.map((d) => d.survival_pct).filter((v) => v != null);
    if (!survPcts.length) return risks;
    const worstPct = Math.min(...survPcts);
    const bestPct = Math.max(...survPcts);
    const level = worstPct < 50 ? 'high' : worstPct < 75 ? 'moderate' : 'low';
    return risks.concat([{
      key: 'maggot',
      name: 'Seedcorn maggot',
      level,
      headline: `Survival ranges ${worstPct}–${bestPct}% across the next ${days.length} days.`,
      detail: 'Day-by-day survival probability for newly-planted seed against seedcorn maggot larvae in the soil — combines degree-day lifecycle (egg hatch → larval feeding window) with soil temperature, moisture, and organic load.',
      metric: `${bestPct}%`,
    }]);
  }

  function wireScmExtendedToggle(card) {
    const btn = card.querySelector('.scm-toggle-btn');
    if (!btn) return;
    const disc = card.querySelector('.scm-extended-actions .extended-disclaimer');
    btn.addEventListener('click', () => {
      const open = card.classList.toggle('is-extended');
      btn.textContent = open ? 'Show 14 days' : 'Show 31 days';
      if (disc) disc.hidden = !open;
    });
  }

  // ----- risk-detail timeline rendering ----------------------------------
  // Each profile picks the metric that most directly drives the evaluator's
  // decision and the threshold the evaluator compares against. The detail
  // panel renders three windows of that metric: prior 30 days from the
  // archive, the 48h evaluation window from hourly forecast, and the next 7
  // days from the daily forecast.

  const RISK_PROFILES = {
    chilling: {
      metric: 'soil_temp',
      threshold: (d) => d.crop.min_soil_temp_f,
      thresholdLabel: (d) => `${d.crop.label}'s ${d.crop.min_soil_temp_f}°F floor`,
      direction: 'above',
    },
    flooding: {
      metric: 'precip_daily', threshold: () => 1.0,
      thresholdLabel: () => 'Caution above 1.0" / day', direction: 'below',
    },
    antecedent: {
      metric: 'precip_cum', threshold: () => 4.0,
      thresholdLabel: () => 'Profile loaded above ~4" / 30d', direction: 'below',
    },
    frost: {
      metric: 'air_min', threshold: (d) => d.crop.frost_air_temp_f,
      thresholdLabel: (d) => `${d.crop.label}'s ${d.crop.frost_air_temp_f}°F frost floor`,
      direction: 'above',
    },
    crusting: {
      metric: 'precip_daily', threshold: () => 0.5,
      thresholdLabel: () => 'Crusting risk above 0.5" / day with hot drying',
      direction: 'below',
    },
    pythium: {
      metric: 'soil_temp', threshold: () => 55,
      thresholdLabel: () => 'P. ultimum aggressive below 55°F + saturated',
      direction: 'above',
    },
    phytophthora: {
      metric: 'soil_temp', threshold: () => 60,
      thresholdLabel: () => 'P. sojae active above 60°F + saturated',
      direction: 'below',
    },
    maggot: {
      metric: 'soil_temp', threshold: () => 65,
      thresholdLabel: () => 'Egg-laying favored below 65°F damp',
      direction: 'above',
    },
    wireworm: {
      metric: 'soil_temp', threshold: () => 65,
      thresholdLabel: () => 'Active feeding below 65°F damp',
      direction: 'above',
    },
    slugs: {
      metric: 'air_temp', threshold: () => 65,
      thresholdLabel: () => 'Slug activity favored below 65°F humid',
      direction: 'above',
    },
    cutworm: {
      metric: 'air_temp', threshold: () => 50,
      thresholdLabel: () => 'GDD base 50°F drives larval development',
      direction: 'above',
    },
    leaf_beetle: {
      metric: 'air_min', threshold: () => 32,
      thresholdLabel: () => 'Frost nights below 32°F kill overwintering adults',
      direction: 'below',
    },
  };

  function renderRiskWindows(key) {
    const profile = RISK_PROFILES[key];
    if (!profile || !latest) return '';
    const ctx = { data: latest, crop: latest.crop };
    const series = METRIC_EXTRACTORS[profile.metric](latest);
    const threshold = profile.threshold(ctx);
    const tLabel = profile.thresholdLabel(ctx);
    const dir = profile.direction;

    return [
      windowCard('Past 30 days', series.prior, series.unit, threshold, tLabel, dir, series.aggregate, 'daily'),
      windowCard('Past 7 days', series.now, series.unit, threshold, tLabel, dir, series.aggregate, 'daily'),
      windowCard('Next 7 days', series.next, series.unit, threshold, tLabel, dir, series.aggregate, 'daily'),
    ].join('');
  }

  function windowCard(title, points, unit, threshold, tLabel, direction, aggregate, granularity) {
    const valid = points.filter((p) => p.v != null && !Number.isNaN(p.v));
    if (!valid.length) {
      return `
        <div class="risk-window">
          <h4>${escapeHTML(title)}</h4>
          <div class="rw-empty muted small">No data for this window.</div>
        </div>`;
    }
    const headline = aggregateValue(valid.map((p) => p.v), aggregate);
    const status = breachStatus(valid, threshold, direction);
    const headlineLabel = aggregateLabel(aggregate);
    const formattedHead = formatVal(headline, unit);
    const subText = subtitleFor(valid, threshold, direction, unit, aggregate);
    const spark = sparkline(valid, threshold, direction, granularity);
    return `
      <div class="risk-window" data-status="${status}">
        <div class="rw-head">
          <h4>${escapeHTML(title)}</h4>
          <span class="rw-agg muted small">${headlineLabel}</span>
        </div>
        <div class="rw-value">${formattedHead}</div>
        ${spark}
        <div class="rw-sub muted small">${escapeHTML(subText)}</div>
        <div class="rw-thresh muted small">${escapeHTML(tLabel)}</div>
      </div>`;
  }

  // ----- per-metric series extractors ------------------------------------

  const METRIC_EXTRACTORS = {
    soil_temp: (data) => ({
      unit: '°F',
      aggregate: 'min',
      prior: zipSeries(data.history_series?.time, data.history_series?.soil_f),
      now: zipSeries(data.history_series?.time, data.history_series?.soil_f).slice(-7),
      next: (data.plant_days || []).slice(0, 7).map((d) => ({
        t: d.date, v: d.conditions?.avg_soil_temp_f,
      })),
    }),
    air_min: (data) => ({
      unit: '°F',
      aggregate: 'min',
      prior: zipSeries(data.history_series?.time, data.history_series?.tmin_f),
      now: zipSeries(data.history_series?.time, data.history_series?.tmin_f).slice(-7),
      next: zipSeries(data.daily?.time?.slice(0, 7), data.daily?.tmin_f?.slice(0, 7)),
    }),
    air_temp: (data) => {
      const tmin = data.history_series?.tmin_f || [];
      const tmax = data.history_series?.tmax_f || [];
      const meanPrior = tmin.map((lo, i) => (lo != null && tmax[i] != null) ? (lo + tmax[i]) / 2 : null);
      const dTmin = data.daily?.tmin_f || [];
      const dTmax = data.daily?.tmax_f || [];
      const meanNext = dTmin.slice(0, 7).map((lo, i) => (lo != null && dTmax[i] != null) ? (lo + dTmax[i]) / 2 : null);
      return {
        unit: '°F',
        aggregate: 'mean',
        prior: zipSeries(data.history_series?.time, meanPrior),
        now: zipSeries(data.history_series?.time, meanPrior).slice(-7),
        next: zipSeries((data.daily?.time || []).slice(0, 7), meanNext),
      };
    },
    precip_daily: (data) => ({
      unit: '"',
      aggregate: 'sum',
      prior: zipSeries(data.history_series?.time, data.history_series?.precip_in),
      now: zipSeries(data.history_series?.time, data.history_series?.precip_in).slice(-7),
      next: zipSeries(data.daily?.time?.slice(0, 7), data.daily?.precip_in?.slice(0, 7)),
    }),
    precip_cum: (data) => {
      const prior = runningSum(zipSeries(data.history_series?.time, data.history_series?.precip_in));
      const nowSeries = runningSum(zipSeries(data.history_series?.time, data.history_series?.precip_in).slice(-7));
      const nextSeries = runningSum(zipSeries(data.daily?.time?.slice(0, 7), data.daily?.precip_in?.slice(0, 7)));
      return { unit: '"', aggregate: 'last', prior, now: nowSeries, next: nextSeries };
    },
  };

  function zipSeries(times, values) {
    if (!times || !values) return [];
    return times.map((t, i) => ({ t, v: values[i] != null ? Number(values[i]) : null }));
  }

  function dailyBucketsFromHourly(times, values, n) {
    if (!times || !values) return [];
    const out = [];
    const len = Math.min(times.length, values.length, n);
    let dayKey = null, sum = 0, count = 0, dayStart = null;
    for (let i = 0; i < len; i++) {
      const k = (times[i] || '').slice(0, 10);
      if (k !== dayKey) {
        if (dayKey != null) out.push({ t: dayStart, v: count ? sum : null });
        dayKey = k; sum = 0; count = 0; dayStart = times[i];
      }
      const v = values[i];
      if (v != null) { sum += Number(v); count++; }
    }
    if (dayKey != null) out.push({ t: dayStart, v: count ? sum : null });
    return out;
  }

  function runningSum(points, start = 0) {
    let acc = start;
    return points.map((p) => {
      if (p.v != null) acc += p.v;
      return { t: p.t, v: acc };
    });
  }

  function aggregateValue(vals, kind) {
    if (!vals.length) return null;
    if (kind === 'sum') return vals.reduce((a, b) => a + b, 0);
    if (kind === 'mean') return vals.reduce((a, b) => a + b, 0) / vals.length;
    if (kind === 'max') return Math.max(...vals);
    if (kind === 'last') return vals[vals.length - 1];
    return Math.min(...vals);  // default 'min'
  }

  function aggregateLabel(kind) {
    return ({ sum: 'total', mean: 'avg', max: 'peak', last: 'cumulative', min: 'low' }[kind] || '');
  }

  function formatVal(v, unit) {
    if (v == null) return '—';
    if (unit === '°F') return `${Math.round(v)}°F`;
    if (unit === '"')  return `${v.toFixed(2)}"`;
    return `${v.toFixed(1)} ${unit}`;
  }

  function breachStatus(points, threshold, direction) {
    if (threshold == null) return 'neutral';
    const breach = points.some((p) => direction === 'above'
      ? p.v < threshold
      : p.v > threshold);
    return breach ? 'breach' : 'ok';
  }

  function subtitleFor(points, threshold, direction, unit, aggregate) {
    if (threshold == null) return '';
    const breaches = points.filter((p) => direction === 'above'
      ? p.v < threshold
      : p.v > threshold);
    if (!breaches.length) {
      const word = direction === 'above' ? 'stayed at or above' : 'stayed at or below';
      return `${points.length}/${points.length} ${aggregate === 'sum' ? 'days' : 'reads'} ${word} ${formatVal(threshold, unit)}`;
    }
    const word = direction === 'above' ? 'below' : 'above';
    return `${breaches.length} ${breaches.length === 1 ? 'reading' : 'readings'} ${word} ${formatVal(threshold, unit)}`;
  }

  function sparkline(points, threshold, direction, granularity) {
    const W = 200, H = 48, PAD = 4;
    const vals = points.map((p) => p.v);
    let lo = Math.min(...vals);
    let hi = Math.max(...vals);
    if (threshold != null) { lo = Math.min(lo, threshold); hi = Math.max(hi, threshold); }
    if (hi - lo < 0.001) { hi = lo + 1; }
    const range = hi - lo;
    const xStep = (W - PAD * 2) / Math.max(1, points.length - 1);
    const yFor = (v) => H - PAD - ((v - lo) / range) * (H - PAD * 2);

    const path = points.map((p, i) => {
      const x = PAD + i * xStep;
      const y = yFor(p.v);
      return `${i === 0 ? 'M' : 'L'}${x.toFixed(1)} ${y.toFixed(1)}`;
    }).join(' ');

    const fillPath = `${path} L${(PAD + (points.length - 1) * xStep).toFixed(1)} ${(H - PAD).toFixed(1)} L${PAD} ${(H - PAD).toFixed(1)} Z`;

    let threshLine = '';
    if (threshold != null) {
      const ty = yFor(threshold);
      threshLine = `<line x1="${PAD}" y1="${ty.toFixed(1)}" x2="${(W - PAD).toFixed(1)}" y2="${ty.toFixed(1)}" class="spark-thresh"/>`;
    }

    const dots = points.map((p, i) => {
      const x = PAD + i * xStep;
      const y = yFor(p.v);
      const breach = threshold != null && (direction === 'above' ? p.v < threshold : p.v > threshold);
      return `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="${breach ? 2.2 : 1.4}" class="spark-dot${breach ? ' is-breach' : ''}"/>`;
    }).join('');

    return `<svg class="spark" viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" aria-hidden="true">
      <path d="${fillPath}" class="spark-fill"/>
      ${threshLine}
      <path d="${path}" class="spark-line"/>
      ${dots}
    </svg>`;
  }


  function renderPlantDays(days, best, crop) {
    if (!days || !days.length) return '<p class="muted">No daily plant scoring available.</p>';
    const bestSet = new Set((best || []).map((b) => b.date));
    const soilFloor = crop?.min_soil_temp_f;
    const frostFloor = crop?.frost_air_temp_f;
    return days.map((d) => {
      const date = parseLocalDate(d.date);
      const dow = date.toLocaleDateString(undefined, { weekday: 'short' });
      const dateLabel = date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
      const tag = bestSet.has(d.date) ? '<span class="best-pill">Best</span>' : '';
      const reasons = (d.top_risks || []).map((r) =>
        `<span class="reason" data-level="${r.level}">${escapeHTML(r.name)}</span>`
      ).join('') || '<span class="reason" data-level="low">All clear</span>';
      const c = d.conditions || {};
      const soilState = c.min_soil_temp_f == null ? ''
        : c.min_soil_temp_f < soilFloor - 3 ? 'high'
        : c.min_soil_temp_f < soilFloor ? 'moderate' : 'low';
      const frostState = c.min_air_temp_f == null ? ''
        : c.min_air_temp_f <= frostFloor - 4 ? 'high'
        : c.min_air_temp_f <= frostFloor ? 'moderate' : 'low';
      const rainState = c.precip_48h_in == null ? ''
        : c.precip_48h_in > 2.0 ? 'high'
        : c.precip_48h_in > 1.0 ? 'moderate' : 'low';
      const satState = c.sat_hours_96h == null ? ''
        : c.sat_hours_96h > 48 ? 'high'
        : c.sat_hours_96h > 24 ? 'moderate' : 'low';
      const conditions = `
        <dl class="pd-conditions">
          <div data-level="${soilState}" title="Min soil temp in the first 48h after planting (floor: ${soilFloor}°F)">
            <dt>Soil min</dt>
            <dd>${c.min_soil_temp_f != null ? c.min_soil_temp_f.toFixed(0) + '°F' : '—'}</dd>
          </div>
          <div data-level="${frostState}" title="Min air temp during the emergence window (floor: ${frostFloor}°F)">
            <dt>Air low</dt>
            <dd>${c.min_air_temp_f != null ? c.min_air_temp_f.toFixed(0) + '°F' : '—'}</dd>
          </div>
          <div data-level="${rainState}" title="Total precipitation in the 48h after planting">
            <dt>Rain 48h</dt>
            <dd>${c.precip_48h_in != null ? c.precip_48h_in.toFixed(2) + '"' : '—'}</dd>
          </div>
          <div data-level="${satState}" title="Hours the topsoil stays above field capacity (96h)">
            <dt>Sat hrs</dt>
            <dd>${c.sat_hours_96h != null ? c.sat_hours_96h + 'h' : '—'}</dd>
          </div>
        </dl>`;
      const climateCls = d.is_climate ? ' is-climate' : '';
      const climateBadge = d.is_climate
        ? '<span class="climate-badge" title="Projected from prior-year climate normals — not from a live forecast">climate avg</span>'
        : '';
      return `
        <div class="plant-day${climateCls}" data-level="${d.level}">
          <div class="pd-head">
            <div>
              <div class="dow">${dow} ${tag}</div>
              <div class="date">${dateLabel}</div>
              ${climateBadge}
            </div>
            <div class="pd-score">${d.score}</div>
          </div>
          <div class="pd-verdict">${escapeHTML(d.verdict)}</div>
          ${conditions}
          <div class="pd-reasons">${reasons}</div>
        </div>`;
    }).join('');
  }

  function renderVerdictConfidence(data) {
    const main = document.querySelector('#verdict .verdict-main');
    if (!main) return;
    let pill = document.getElementById('verdictConfidence');
    if (!pill) {
      pill = document.createElement('div');
      pill.id = 'verdictConfidence';
      pill.className = 'verdict-confidence';
      main.appendChild(pill);
    }
    const surv = data.survival;
    const conf = data.forecast_confidence;
    if (!surv && !conf) { pill.innerHTML = ''; return; }

    const range = surv && surv.low_pct != null && surv.high_pct != null
      ? `${surv.low_pct}–${surv.high_pct}%`
      : (surv && surv.point_pct != null ? `${surv.point_pct}%` : '—');
    const point = surv && surv.point_pct != null ? `${surv.point_pct}%` : '—';
    const label = conf && conf.label ? conf.label : '—';
    const tone = label === 'high' ? 'good' : (label === 'low' ? 'warn' : '');
    const drivers = (conf && conf.drivers) || [];

    pill.dataset.tone = tone;
    pill.innerHTML = `
      <div class="vc-row">
        <span class="vc-key">Survival probability (today)</span>
        <span class="vc-val">${point} <span class="muted">· interval ${range}</span></span>
      </div>
      <div class="vc-row">
        <span class="vc-key">Forecast confidence</span>
        <span class="vc-val">${escapeHTML(label[0] ? label[0].toUpperCase() + label.slice(1) : '—')}${conf && conf.scalar != null ? ` <span class="muted">(${(conf.scalar * 100).toFixed(0)}/100)</span>` : ''}</span>
      </div>
      ${drivers.length ? `<ul class="vc-drivers">${drivers.map((d) => `<li>${escapeHTML(d)}</li>`).join('')}</ul>` : ''}
    `;
  }

  function renderSeed(seed, crop) {
    const panel = document.getElementById('seedPanel');
    const root = document.getElementById('seedContent');
    if (!panel || !root) return;
    if (!seed || !seed.brand) {
      panel.hidden = true;
      root.innerHTML = '';
      return;
    }
    panel.hidden = false;

    const traits = (seed.traits || []).map((t) =>
      `<span class="seed-trait">${escapeHTML(t)}</span>`).join('') || '';

    const stats = [];
    if (seed.rm != null)            stats.push(['Maturity', `${seed.rm} ${crop?.key === 'corn' ? 'day' : 'group'}`]);
    if (seed.cold_tolerance)        stats.push(['Cold tolerance', cap(seed.cold_tolerance)]);
    if (seed.emergence_score != null) stats.push(['Stress emergence', `${seed.emergence_score}/9`]);
    if (seed.phytophthora)          stats.push(['Phytophthora', seed.phytophthora]);
    if (seed.scn_source)            stats.push(['SCN source', seed.scn_source]);
    if (seed.idc != null)           stats.push(['IDC score', `${seed.idc}/9`]);

    const statsHtml = `
      <dl class="seed-stats">
        ${stats.map(([k, v]) =>
          `<div><dt>${escapeHTML(k)}</dt><dd>${escapeHTML(String(v))}</dd></div>`
        ).join('')}
      </dl>`;

    const diffs = seed.threshold_diffs || {};
    const diffLabels = {
      min_soil_temp_f: 'Soil-temp floor',
      preferred_soil_temp_f: 'Preferred soil temp',
      frost_air_temp_f: 'Frost air floor',
      phytophthora_sensitive: 'Phytophthora penalised',
    };
    const diffRows = Object.keys(diffs).map((k) => {
      const d = diffs[k];
      const fmt = (v) => typeof v === 'number' ? `${v}°F` : (v === true ? 'yes' : v === false ? 'no' : '—');
      return `
        <tr>
          <td>${escapeHTML(diffLabels[k] || k)}</td>
          <td><span class="muted">${fmt(d.base)}</span></td>
          <td><strong>${fmt(d.tailored)}</strong></td>
        </tr>`;
    }).join('');
    const diffsHtml = diffRows
      ? `<table class="seed-diffs"><thead><tr><th>Threshold</th><th>Crop default</th><th>Tailored</th></tr></thead><tbody>${diffRows}</tbody></table>`
      : '<p class="muted small seed-no-diffs">Cultivar trait class matches the crop defaults — no threshold shifts applied.</p>';

    const notes = (seed.tailoring_notes || []).map((n) =>
      `<li>${formatMarkdownBold(n)}</li>`).join('');
    const notesHtml = notes
      ? `<div class="seed-notes"><h4>What changed for this cultivar</h4><ul>${notes}</ul></div>`
      : '';

    const blurb = seed.notes
      ? `<p class="seed-blurb">${escapeHTML(seed.notes)}</p>`
      : '';

    root.innerHTML = `
      <header class="seed-card-head">
        <div>
          <p class="seed-eyebrow muted small">${escapeHTML((crop?.label || '').toUpperCase())}</p>
          <h3 class="seed-title">${escapeHTML(seed.brand)} · ${escapeHTML(seed.cultivar_id)}</h3>
        </div>
        <div class="seed-traits">${traits}</div>
      </header>
      ${blurb}
      ${statsHtml}
      ${diffsHtml}
      ${notesHtml}
      <p class="muted small seed-foot">All risk evaluators ran against the tailored thresholds above. Choosing a different cultivar on the dashboard re-runs the model.</p>
    `;
  }

  function cap(s) { return s ? s.charAt(0).toUpperCase() + s.slice(1) : ''; }

  // Allows our short tailoring notes to use **bold** for the changed values.
  function formatMarkdownBold(s) {
    return escapeHTML(s).replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
  }

  function renderDataSources(data) {
    const panel = document.getElementById('sourcesPanel');
    const root = document.getElementById('sourcesContent');
    if (!panel || !root) return;
    const ds = data.data_sources;
    if (!ds) { panel.hidden = true; return; }
    panel.hidden = false;

    const cards = [];

    // 0. Forecast confidence — first card so the user sees it before details.
    const fc = ds.forecast_confidence;
    if (fc && fc.available && fc.label) {
      const tone = fc.label === 'high' ? 'good' : (fc.label === 'low' ? 'warn' : '');
      const weak = (fc.weak_signals || []).length
        ? ` Weak signals: ${(fc.weak_signals || []).map(escapeHTML).join(', ')}.`
        : '';
      cards.push(`
        <article class="src-card" data-tone="${tone}">
          <header class="src-head">
            <h3>Forecast confidence</h3>
            <span class="src-link muted">aggregate</span>
          </header>
          <p class="src-headline">${escapeHTML(fc.label[0].toUpperCase() + fc.label.slice(1))} confidence (${(fc.scalar * 100).toFixed(0)}/100)</p>
          <dl class="src-stats">
            <div><dt>Forward agreement</dt><dd>${escapeHTML(fc.agreement_forward || '—')}</dd></div>
            <div><dt>Recent agreement</dt><dd>${escapeHTML(fc.agreement_recent || '—')}</dd></div>
            <div><dt>Ensemble σ (tmin)</dt><dd>${fc.ensemble_spread_f != null ? `${fc.ensemble_spread_f.toFixed(1)}°F` : '—'}</dd></div>
            <div><dt>Ensemble σ (precip)</dt><dd>${fc.ensemble_precip_spread_in != null ? `${fc.ensemble_precip_spread_in.toFixed(2)}"` : '—'}</dd></div>
          </dl>
          <p class="src-foot muted small">Aggregates Open-Meteo↔NWS, Open-Meteo↔NASA POWER, and ensemble dispersion.${weak}</p>
        </article>`);
    }

    // 0b. Ensemble forecast spread — per-day frost and chilling probability.
    const ens = ds.ensemble_forecast;
    if (ens && ens.available && (ens.daily || []).length) {
      const days = ens.daily.slice(0, 7);
      const rows = days.map((d) => {
        const dt = parseLocalDate(d.date);
        const lbl = dt.toLocaleDateString(undefined, { weekday: 'short', month: 'short', day: 'numeric' });
        const fp = d.frost_prob != null ? `${(d.frost_prob * 100).toFixed(0)}%` : '—';
        const cp = d.chilling_prob != null ? `${(d.chilling_prob * 100).toFixed(0)}%` : '—';
        const wp = d.wet_prob != null ? `${(d.wet_prob * 100).toFixed(0)}%` : '—';
        const tminStd = d.tmin_std_f != null ? `±${d.tmin_std_f.toFixed(1)}` : '—';
        const tmaxStd = d.tmax_std_f != null ? `±${d.tmax_std_f.toFixed(1)}` : '—';
        return `<tr><td>${lbl}</td><td>${fp}</td><td>${cp}</td><td>${wp}</td><td>${tminStd} / ${tmaxStd}</td></tr>`;
      }).join('');
      const headlineFreeze = days.find((d) => d.frost_prob != null && d.frost_prob >= 0.3);
      const headlineChill = days.find((d) => d.chilling_prob != null && d.chilling_prob >= 0.3);
      const tone = (headlineFreeze || headlineChill) ? 'warn' : 'good';
      const headline = headlineFreeze
        ? `Ensemble shows ≥30% freeze on at least one day in the next 7.`
        : headlineChill
          ? `Ensemble shows ≥30% imbibitional-chilling soil on at least one day in the next 7.`
          : `Ensemble members agree on a non-stress next 7 days.`;
      cards.push(`
        <article class="src-card" data-tone="${tone}">
          <header class="src-head">
            <h3>Ensemble forecast (${ens.members || 0} members)</h3>
            <a class="src-link" href="${escapeAttr(ds.ensemble_forecast.url || 'https://open-meteo.com/en/docs/ensemble-api')}" target="_blank" rel="noopener">Open-Meteo Ensemble ↗</a>
          </header>
          <p class="src-headline">${escapeHTML(headline)}</p>
          <p class="src-sub muted small">Models: ${(ens.models || []).join(', ')}</p>
          <details class="src-details"><summary>Per-day probabilities</summary>
            <table class="src-table">
              <thead><tr><th>Date</th><th>P[freeze]</th><th>P[chilling]</th><th>P[≥0.5" rain]</th><th>σ tmin / tmax</th></tr></thead>
              <tbody>${rows}</tbody>
            </table>
          </details>
        </article>`);
    } else if (ens) {
      cards.push(unavailableCard('Ensemble forecast', 'Open-Meteo Ensemble',
        ens.url, 'Ensemble service unavailable for this point.'));
    }

    // 0c. NASA POWER recent actuals + three-source agreement.
    const ra = ds.recent_actuals_cross_check;
    if (ra && ra.available && (ra.daily || []).length) {
      const tone = ra.agreement === 'strong' ? 'good'
                : ra.agreement === 'fair' ? '' : 'warn';
      const headline = ra.agreement === 'strong'
        ? `Open-Meteo Archive and NASA POWER agree on the last week.`
        : ra.agreement === 'fair'
          ? `Reanalyses agree on temperature, modest precip drift.`
          : `Reanalyses disagree on the last week — antecedent saturation is uncertain.`;
      const rows = (ra.days || []).slice(-7).map((d) => {
        const dt = parseLocalDate(d.date);
        const lbl = dt.toLocaleDateString(undefined, { weekday: 'short', month: 'short', day: 'numeric' });
        const f = (v) => v == null ? '—' : `${v.toFixed(0)}°`;
        const p = (v) => v == null ? '—' : `${v.toFixed(2)}"`;
        const dev = (v, unit) => v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(unit === '°' ? 0 : 2)}${unit}`;
        return `<tr>
          <td>${lbl}</td>
          <td>${f(d.open_meteo_tmin_f)} / ${f(d.open_meteo_tmax_f)}</td>
          <td>${f(d.power_tmin_f)} / ${f(d.power_tmax_f)}</td>
          <td>${p(d.open_meteo_precip_in)} / ${p(d.power_precip_in)}</td>
          <td>${dev(d.tmin_dev_f, '°')} / ${dev(d.tmax_dev_f, '°')} / ${dev(d.precip_dev_in, '"')}</td>
        </tr>`;
      }).join('');
      cards.push(`
        <article class="src-card" data-tone="${tone}">
          <header class="src-head">
            <h3>Recent-actuals cross-check</h3>
            <a class="src-link" href="${escapeAttr(ra.url || 'https://power.larc.nasa.gov/')}" target="_blank" rel="noopener">NASA POWER ↗</a>
          </header>
          <p class="src-headline">${escapeHTML(headline)}</p>
          <p class="src-sub muted small">Mean dev: tmin ${ra.mean_dev_tmin_f != null ? `${ra.mean_dev_tmin_f.toFixed(1)}°F` : '—'} · tmax ${ra.mean_dev_tmax_f != null ? `${ra.mean_dev_tmax_f.toFixed(1)}°F` : '—'} · precip ${ra.mean_dev_precip_in != null ? `${ra.mean_dev_precip_in.toFixed(2)}"` : '—'}</p>
          <details class="src-details"><summary>Day-by-day comparison</summary>
            <table class="src-table">
              <thead><tr><th>Date</th><th>OM lo/hi</th><th>POWER lo/hi</th><th>OM rain / POWER rain</th><th>Δ lo / Δ hi / Δ p</th></tr></thead>
              <tbody>${rows}</tbody>
            </table>
          </details>
        </article>`);
    } else if (ra) {
      cards.push(unavailableCard('Recent-actuals cross-check', 'NASA POWER',
        ra.url, 'NASA POWER reanalysis unavailable for this point.'));
    }

    // 0d. Topography / ponding risk.
    const topo = ds.topography;
    if (topo && topo.available && topo.data) {
      const t = topo.data;
      const tone = t.ponding_risk === 'high' ? 'warn'
                : t.ponding_risk === 'low' ? 'good' : '';
      const concSign = t.concavity_m >= 0 ? '+' : '';
      cards.push(`
        <article class="src-card" data-tone="${tone}">
          <header class="src-head">
            <h3>Topography &amp; ponding</h3>
            <a class="src-link" href="${escapeAttr(topo.url)}" target="_blank" rel="noopener">Open-Meteo Elevation ↗</a>
          </header>
          <p class="src-headline">Ponding risk: ${escapeHTML(t.ponding_risk)}</p>
          <dl class="src-stats">
            <div><dt>Center elev.</dt><dd>${t.center_elev_m.toFixed(1)} m</dd></div>
            <div><dt>Mean neighbour elev.</dt><dd>${t.mean_neighbor_elev_m.toFixed(1)} m</dd></div>
            <div><dt>Concavity</dt><dd>${concSign}${t.concavity_m.toFixed(2)} m</dd></div>
            <div><dt>Slope</dt><dd>${t.slope_m_per_km.toFixed(1)} m/km</dd></div>
          </dl>
          <p class="src-foot muted small">Center sits ${t.concavity_m >= 0 ? 'below' : 'above'} its 8 neighbours by ${Math.abs(t.concavity_m).toFixed(2)} m on a ${t.slope_m_per_km.toFixed(1)} m/km slope. Bowl-shaped microsites pond water beyond what the modeled saturation can predict.</p>
        </article>`);
    } else if (topo) {
      cards.push(unavailableCard('Topography & ponding', 'Open-Meteo Elevation',
        topo.url, 'Elevation lookup unavailable for this point.'));
    }


    // 1. Soil profile (SSURGO)
    const soil = ds.soil_profile;
    if (soil && soil.available && soil.data) {
      const s = soil.data;
      const drainTone = /poorly drained/i.test(s.drainage_class || '') ? 'warn'
                       : /well drained/i.test(s.drainage_class || '') ? 'good' : '';
      const texture = s.texture_class || '—';
      const om = s.organic_matter_pct != null ? `${s.organic_matter_pct.toFixed(1)}%` : '—';
      const fines = (s.sand_pct != null && s.silt_pct != null && s.clay_pct != null)
        ? `${s.sand_pct.toFixed(0)}/${s.silt_pct.toFixed(0)}/${s.clay_pct.toFixed(0)}` : '—';
      cards.push(`
        <article class="src-card" data-tone="${drainTone}">
          <header class="src-head">
            <h3>Soil profile</h3>
            <a class="src-link" href="${escapeAttr(soil.url)}" target="_blank" rel="noopener">USDA SSURGO ↗</a>
          </header>
          <p class="src-headline">${escapeHTML(s.map_unit || s.component || 'Soil mapped')}</p>
          <dl class="src-stats">
            <div><dt>Drainage</dt><dd>${escapeHTML(s.drainage_class || '—')}</dd></div>
            <div><dt>Hydrologic group</dt><dd>${escapeHTML(s.hydrologic_group || '—')}</dd></div>
            <div><dt>Texture</dt><dd>${escapeHTML(texture)}</dd></div>
            <div><dt>Sand / silt / clay</dt><dd>${fines}</dd></div>
            <div><dt>Organic matter</dt><dd>${om}</dd></div>
            <div><dt>Avail. water cap.</dt><dd>${s.available_water_capacity != null ? s.available_water_capacity.toFixed(2) : '—'}</dd></div>
          </dl>
          <p class="src-foot muted small">Folded into Phytophthora drainage, Pythium texture, antecedent buffer, and crusting.</p>
        </article>`);
    } else if (soil) {
      cards.push(unavailableCard('Soil profile', 'USDA SSURGO',
        soil.url, 'Outside SSURGO coverage or service offline.'));
    }

    // 2. Forecast cross-check (Open-Meteo vs NWS)
    const xc = ds.forecast_cross_check;
    if (xc && xc.available) {
      const tone = xc.agreement === 'strong' ? 'good'
                : xc.agreement === 'fair' ? '' : 'warn';
      const headline = xc.agreement === 'strong'
        ? `Open-Meteo and NWS agree closely`
        : xc.agreement === 'fair'
          ? `Models broadly agree (mean ${xc.mean_dev_f}°F dev)`
          : `Models disagree noticeably (max ${xc.max_dev_f}°F dev)`;
      const rows = (xc.days || []).slice(0, 5).map((d) => {
        const date = parseLocalDate(d.date);
        const lbl = date.toLocaleDateString(undefined, { weekday: 'short', month: 'short', day: 'numeric' });
        const fmtDev = (v) => v == null ? '—' : (v >= 0 ? `+${v.toFixed(0)}` : v.toFixed(0));
        const fmt = (v) => v == null ? '—' : `${v.toFixed(0)}°`;
        const fmtPrecip = (om, pop) => {
          const omS = om == null ? '—' : `${om.toFixed(2)}"`;
          const popS = pop == null ? '—' : `${pop}%`;
          return `${omS} / ${popS}`;
        };
        const flag = d.precip_disagreement ? ' ⚠' : '';
        return `<tr>
          <td>${lbl}</td>
          <td>${fmt(d.open_meteo_tmax_f)} / ${fmt(d.open_meteo_tmin_f)}</td>
          <td>${fmt(d.nws_tmax_f)} / ${fmt(d.nws_tmin_f)}</td>
          <td>${fmtDev(d.tmax_dev_f)} / ${fmtDev(d.tmin_dev_f)}</td>
          <td>${fmtPrecip(d.open_meteo_precip_in, d.nws_precip_pop)}${flag}</td>
        </tr>`;
      }).join('');
      cards.push(`
        <article class="src-card" data-tone="${tone}">
          <header class="src-head">
            <h3>Forecast cross-check</h3>
            <a class="src-link" href="${escapeAttr(xc.url || ds.forecast_cross_check.url)}" target="_blank" rel="noopener">NWS ↗</a>
          </header>
          <p class="src-headline">${escapeHTML(headline)}</p>
          <p class="src-sub muted small">NWS office ${escapeHTML(xc.office || '—')} · mean dev ${xc.mean_dev_f}°F · max ${xc.max_dev_f}°F over next ${(xc.days || []).length}d${xc.precip_disagreement_days ? ` · ${xc.precip_disagreement_days} day${xc.precip_disagreement_days === 1 ? '' : 's'} POP/precip mismatch ⚠` : ''}</p>
          <details class="src-details"><summary>Day-by-day comparison</summary>
            <table class="src-table">
              <thead><tr><th>Date</th><th>Open-Meteo (hi/lo)</th><th>NWS (hi/lo)</th><th>Δ (hi/lo)</th><th>OM rain / NWS POP</th></tr></thead>
              <tbody>${rows}</tbody>
            </table>
          </details>
        </article>`);
    } else if (ds.forecast_cross_check) {
      cards.push(unavailableCard('Forecast cross-check', 'NWS',
        ds.forecast_cross_check.url, 'NWS forecast unavailable for this point.'));
    }

    // 3. NWS active alerts (flood/freeze watches & warnings)
    const al = ds.weather_alerts;
    if (al && al.available) {
      if (al.count > 0) {
        const tone = (al.any_flood || al.any_freeze) ? 'warn' : '';
        const headline = al.count === 1
          ? `1 active NWS alert at this point`
          : `${al.count} active NWS alerts at this point`;
        const items = (al.alerts || []).map((a) => {
          const expires = a.expires ? ` · expires ${formatAlertExpires(a.expires)}` : '';
          const sev = a.severity ? ` (${escapeHTML(a.severity)})` : '';
          return `<li><strong>${escapeHTML(a.event)}</strong>${sev}${expires}<br><span class="muted small">${escapeHTML(a.headline || a.area_desc || '')}</span></li>`;
        }).join('');
        cards.push(`
          <article class="src-card" data-tone="${tone}">
            <header class="src-head">
              <h3>Active weather alerts</h3>
              <a class="src-link" href="${escapeAttr(al.url)}" target="_blank" rel="noopener">NWS ↗</a>
            </header>
            <p class="src-headline">${escapeHTML(headline)}</p>
            <ul class="src-list">${items}</ul>
            <p class="src-foot muted small">Flood watches/warnings escalate the flooding evaluator; frost/freeze advisories escalate the frost evaluator.</p>
          </article>`);
      } else {
        cards.push(`
          <article class="src-card" data-tone="good">
            <header class="src-head">
              <h3>Active weather alerts</h3>
              <a class="src-link" href="${escapeAttr(al.url)}" target="_blank" rel="noopener">NWS ↗</a>
            </header>
            <p class="src-headline">No active NWS alerts at this point.</p>
            <p class="src-sub muted small">Frost, freeze, flood, and severe-weather alerts are checked live and surfaced here when active.</p>
          </article>`);
      }
    } else if (al) {
      cards.push(unavailableCard('Active weather alerts', 'NWS',
        al.url, 'NWS alerts feed unavailable for this point.'));
    }

    // 4. U.S. Drought Monitor — current weekly classification
    const dr = ds.drought;
    if (dr && dr.available && dr.data) {
      const cls = dr.data.class;
      const tone = cls >= 2 ? 'warn' : (cls === -1 ? 'good' : '');
      const mapDate = dr.data.map_date ? ` (map ${dr.data.map_date})` : '';
      cards.push(`
        <article class="src-card" data-tone="${tone}">
          <header class="src-head">
            <h3>Drought status</h3>
            <a class="src-link" href="${escapeAttr(dr.url)}" target="_blank" rel="noopener">U.S. Drought Monitor ↗</a>
          </header>
          <p class="src-headline">${escapeHTML(dr.data.label || 'Unknown')}${escapeHTML(mapDate)}</p>
          <p class="src-sub muted small">${cls >= 2 ? 'Drought down-regulates Pythium pressure (no prolonged saturation tail).' : 'No drought-driven adjustment to pathogen scoring.'}</p>
        </article>`);
    } else if (dr) {
      cards.push(unavailableCard('Drought status', 'U.S. Drought Monitor',
        dr.url, 'USDM service unavailable or point outside coverage.'));
    }

    // 5. Black-cutworm biofix (ISU)
    const bcw = ds.bcw_biofix;
    if (bcw && bcw.available) {
      const flightDate = bcw.earliest_flight_iso ? parseLocalDate(bcw.earliest_flight_iso) : null;
      const flightLbl = flightDate ? flightDate.toLocaleDateString(undefined, { month: 'long', day: 'numeric', year: 'numeric' }) : '—';
      cards.push(`
        <article class="src-card">
          <header class="src-head">
            <h3>Black-cutworm biofix</h3>
            <a class="src-link" href="${escapeAttr(bcw.url)}" target="_blank" rel="noopener">ISU ICM ↗</a>
          </header>
          <p class="src-headline">First significant flight: ${escapeHTML(flightLbl)}</p>
          <p class="src-sub muted small">${bcw.counties_reported} Iowa ${bcw.counties_reported === 1 ? 'county' : 'counties'} reporting · earliest used as upper-Midwest seed for the GDD-since-flight model.</p>
          <p class="src-foot muted small">Replaces the default mid-April biofix in the cutworm pressure scoring.</p>
        </article>`);
    } else if (bcw) {
      cards.push(unavailableCard('Black-cutworm biofix', 'ISU ICM',
        bcw.url, 'ISU report not yet posted for this season — using default mid-April biofix.'));
    }

    // 4. Primary forecast attribution
    if (ds.primary_forecast) {
      cards.push(`
        <article class="src-card">
          <header class="src-head">
            <h3>Primary forecast</h3>
            <a class="src-link" href="${escapeAttr(ds.primary_forecast.url)}" target="_blank" rel="noopener">Open-Meteo ↗</a>
          </header>
          <p class="src-sub muted small">${escapeHTML(ds.primary_forecast.covers || '')}</p>
        </article>`);
    }

    root.innerHTML = cards.join('');
  }

  function unavailableCard(title, label, url, reason) {
    return `
      <article class="src-card src-card-unavail">
        <header class="src-head">
          <h3>${escapeHTML(title)}</h3>
          ${url ? `<a class="src-link" href="${escapeAttr(url)}" target="_blank" rel="noopener">${escapeHTML(label)} ↗</a>` : ''}
        </header>
        <p class="src-sub muted small">${escapeHTML(reason)}</p>
      </article>`;
  }

  function escapeAttr(s) { return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }

  function formatAlertExpires(iso) {
    if (!iso) return '';
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return '';
    return d.toLocaleString(undefined, {
      weekday: 'short', month: 'short', day: 'numeric',
      hour: 'numeric', minute: '2-digit',
    });
  }

  function renderDepth(depth, crop) {
    const panel = document.getElementById('depthPanel');
    const root = document.getElementById('depthContent');
    if (!depth || !panel || !root) return;
    panel.hidden = false;

    const min = depth.min_in;
    const max = depth.max_in;
    const rec = depth.recommended_in;
    const drivers = depth.drivers || {};
    const notes = (depth.notes || []).map((n) => `<li>${escapeHTML(n)}</li>`).join('');

    const recHtml = depth.deferred
      ? `<div class="depth-rec deferred"><span class="depth-label">Recommendation</span>
           <span class="depth-value">Defer planting</span></div>`
      : `<div class="depth-rec"><span class="depth-label">Recommended depth</span>
           <span class="depth-value">${rec != null ? rec.toFixed(2) + '"' : '—'}</span>
           <span class="depth-range muted small">${crop?.label || 'Crop'} window: ${min}"–${max}"</span></div>`;

    const driversHtml = `
      <dl class="depth-drivers">
        <div><dt>Avg soil temp (96h)</dt><dd>${drivers.avg_soil_temp_f != null ? drivers.avg_soil_temp_f.toFixed(0) + '°F' : '—'}</dd></div>
        <div><dt>Topsoil moisture</dt><dd>${drivers.avg_topsoil_moisture != null ? (drivers.avg_topsoil_moisture * 100).toFixed(0) + '%' : '—'}</dd></div>
        <div><dt>Subsoil moisture (3-9cm)</dt><dd>${drivers.avg_subsoil_moisture != null ? (drivers.avg_subsoil_moisture * 100).toFixed(0) + '%' : '—'}</dd></div>
        <div><dt>Rain next 24h</dt><dd>${drivers.rain_24h_in != null ? drivers.rain_24h_in.toFixed(2) + '"' : '—'}</dd></div>
        <div><dt>Rain next 7d</dt><dd>${drivers.rain_7d_in != null ? drivers.rain_7d_in.toFixed(2) + '"' : '—'}</dd></div>
      </dl>`;

    const notesHtml = notes
      ? `<div class="depth-notes"><h4>Why this depth</h4><ul>${notes}</ul></div>`
      : '<div class="depth-notes muted small">Conditions land in the standard depth window — no adjustment needed.</div>';

    root.innerHTML = recHtml + driversHtml + notesHtml;
  }

  function renderDaily(d) {
    if (!d || !d.time || !d.time.length) return '<p class="muted">No daily forecast available.</p>';
    return d.time.map((iso, i) => {
      const date = parseLocalDate(iso);
      const dow = date.toLocaleDateString(undefined, { weekday: 'short' });
      const dateLabel = date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
      const survival = d.survival_pct?.[i];
      const survivalLevel = survival == null ? '' : survival >= 85 ? 'low' : survival >= 60 ? 'moderate' : 'high';
      const survivalRow = survival == null
        ? '<div class="survival muted">survival —</div>'
        : `<div class="survival" data-level="${survivalLevel}">${survival}% survival</div>`;

      // High/low vs same-date prior-year normal (5 yr avg). Positive = warmer than typical.
      const normMax = d.normal_tmax_f?.[i];
      const normMin = d.normal_tmin_f?.[i];
      const tmaxV = d.tmax_f?.[i];
      const tminV = d.tmin_f?.[i];
      let vsNormal = '';
      if (normMax != null && normMin != null && tmaxV != null && tminV != null) {
        const meanForecast = (tmaxV + tminV) / 2;
        const meanNormal = (normMax + normMin) / 2;
        const delta = meanForecast - meanNormal;
        const sign = delta >= 0 ? '+' : '';
        const cls = Math.abs(delta) < 3 ? 'muted' : (delta > 0 ? 'warm' : 'cool');
        vsNormal = `<div class="vs-normal ${cls}">${sign}${delta.toFixed(0)}° vs normal</div>`;
      }

      const isClimate = !!(d.is_climate && d.is_climate[i]);
      const climateCls = isClimate ? ' is-climate' : '';
      const tempsTxt = (tmaxV != null && tminV != null)
        ? `${Math.round(tmaxV)}° / ${Math.round(tminV)}°`
        : '— / —';
      const precipTxt = d.precip_in?.[i] != null
        ? `${d.precip_in[i].toFixed(2)}" rain`
        : '— rain';
      const uvTxt = isClimate ? '' : `<div class="uv">UV ${(d.uv_max?.[i] ?? 0).toFixed(1)}</div>`;
      const climateBadge = isClimate
        ? '<div class="climate-badge" title="Prior-year climate average — not a live forecast">climate avg</div>'
        : '';

      return `
        <div class="day${climateCls}">
          <div class="dow">${dow}</div>
          <div class="date">${dateLabel}</div>
          ${climateBadge}
          <div class="temps">${tempsTxt}</div>
          <div class="precip">${precipTxt}</div>
          ${uvTxt}
          ${vsNormal}
          ${survivalRow}
        </div>`;
    }).join('');
  }

  function renderScmDetail(scm) {
    const season = scm.season || {};
    const peaks = season.peaks_gdd || [];
    const cur = season.gdd_today;
    const seasonLabel = `${cur} DD base ${season.gdd_base_f}°F since Jan 1 · ${season.generation_label || ''}`;

    const maggotPeaks = season.maggot_peaks_gdd || [];
    const summary = [];
    if (season.dd_to_next_maggot_peak != null && season.next_maggot_peak_gdd != null) {
      const idx = maggotPeaks.indexOf(season.next_maggot_peak_gdd);
      const gen = ['1st', '2nd', '3rd'][idx >= 0 ? idx : 0] || '';
      summary.push(scmStat(
        `${season.dd_to_next_maggot_peak} DD`,
        `to next ${gen}-gen maggot peak (${season.next_maggot_peak_gdd} DD)`,
        season.fly_generation_label || ''
      ));
    } else {
      summary.push(scmStat('Past', 'all maggot feeding waves', 'late-season threat tapering'));
    }

    // Best/worst summaries use the forecast-grade window only (climate-projected
    // days past day 14 are too uncertain to call out as "best" or "worst").
    const horizonLabel = `next ${latest?.forecast_horizon_days || 14} days`;
    if (scm.best_day) {
      const d = parseLocalDate(scm.best_day);
      const best = scm.days.find(x => x.date === scm.best_day);
      summary.push(scmStat(
        `${best?.survival_pct ?? '—'}%`,
        `best survival, ${horizonLabel}`,
        d.toLocaleDateString(undefined, { weekday: 'short', month: 'short', day: 'numeric' }),
        'good'
      ));
    }
    if (scm.worst_day && scm.worst_day !== scm.best_day) {
      const d = parseLocalDate(scm.worst_day);
      const worst = scm.days.find(x => x.date === scm.worst_day);
      summary.push(scmStat(
        `${worst?.survival_pct ?? '—'}%`,
        `lowest survival, ${horizonLabel}`,
        d.toLocaleDateString(undefined, { weekday: 'short', month: 'short', day: 'numeric' }),
        'bad'
      ));
    }
    if (scm.organic_load) {
      summary.push(scmStat(
        '!',
        'manure / heavy residue attracts flies — more maggots in soil',
        'consider an insecticide-treated seed',
        'warn'
      ));
    }

    const daysHtml = scm.days.map(scmDayCard).join('');

    const conf = season.confidence;
    const confNote = conf === 'low'
      ? ' · Low confidence — limited season history available'
      : conf === 'moderate' ? ' · Moderate confidence — partial archive coverage' : '';
    const foot =
      `Maggot damage peaks ~150 DD after each fly generation (354 / 1080 / 1800 DD base 39°F).` +
      ` Survival projects against larval feeding in soil — not all risks combined.${confNote}`;

    const hasExtended = scm.days.some((d) => d.is_climate);
    const toggleHtml = hasExtended
      ? `<div class="scm-extended-actions">
           <button type="button" class="btn ghost small scm-toggle-btn">Show 31 days</button>
           <span class="extended-disclaimer muted small" hidden>Less accurate after 14 days</span>
         </div>`
      : '';

    return `
      <p class="muted small scm-season-label">${escapeHTML(seasonLabel)}</p>
      <div class="scm-summary">${summary.join('')}</div>
      <div class="scm-days">${daysHtml}</div>
      ${toggleHtml}
      <p class="muted small scm-foot">${escapeHTML(foot)}</p>
    `;
  }

  function scmDayCard(d) {
    const date = parseLocalDate(d.date);
    const dow = date.toLocaleDateString(undefined, { weekday: 'short' });
    const dl  = date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
    // Tighten the long activity label so it fits on one line in a compact card.
    const flyShort = (d.fly_activity || '').replace(/\s*fly activity$/i, '').trim() || '—';
    const maggotShort = (d.maggot_activity || '').replace(/\s*in soil$/i, '').trim() || '—';
    const phaseShort = compactPhase(d.phase);
    const climateCls = d.is_climate ? ' is-climate' : '';
    const climateBadge = d.is_climate
      ? '<span class="climate-badge" title="Projected from prior-year climate normals — not from a live forecast">climate avg</span>'
      : '';
    const soil = d.avg_soil_f != null ? `${d.avg_soil_f.toFixed(0)}°F` : '—';
    return `
      <div class="scm-day${climateCls}" data-level="${d.level}">
        <div class="scm-day-head">
          <div class="scm-day-when">
            <div class="dow">${dow}</div>
            <div class="date">${dl}</div>
            ${climateBadge}
          </div>
          <div class="scm-day-survival">
            <div class="scm-survival-pct">${d.survival_pct}%</div>
            <div class="scm-survival-label">survival</div>
          </div>
        </div>
        <div class="scm-meter" title="Survival probability vs. seedcorn maggot only">
          <div class="scm-meter-fill" style="width:${d.survival_pct}%"></div>
        </div>
        <ul class="scm-day-stats">
          <li title="Larval (maggot) feeding pressure in soil — the stage that damages seeds">
            <span class="scm-stat-key">Maggots</span>
            <span class="scm-stat-val">${escapeHTML(maggotShort)}</span>
          </li>
          <li title="${escapeHTML(d.phase || '')}">
            <span class="scm-stat-key">Phase</span>
            <span class="scm-stat-val">${escapeHTML(phaseShort)}</span>
          </li>
          <li title="Adult fly activity above ground (egg-laying drives future maggot waves)">
            <span class="scm-stat-key">Flies</span>
            <span class="scm-stat-val">${escapeHTML(flyShort)}</span>
          </li>
          <li title="Estimated days from planting to seedling emergence at this soil temperature">
            <span class="scm-stat-key">Germ</span>
            <span class="scm-stat-val">${d.germination_days}d</span>
          </li>
          <li title="Average soil temp (6cm) over the 96h post-planting window">
            <span class="scm-stat-key">Soil</span>
            <span class="scm-stat-val">${soil}</span>
          </li>
        </ul>
      </div>`;
  }

  function scmStat(value, label, sub, tone) {
    const cls = tone ? ` scm-stat-${tone}` : '';
    return `
      <div class="scm-stat${cls}">
        <div class="scm-stat-value">${escapeHTML(String(value))}</div>
        <div class="scm-stat-label">${escapeHTML(label)}</div>
        ${sub ? `<div class="scm-stat-sub muted">${escapeHTML(sub)}</div>` : ''}
      </div>`;
  }

  function renderHistory(history, climatology, daily) {
    const panel = document.getElementById('historyPanel');
    const content = document.getElementById('historyContent');
    if (!history || history.cumulative_precip_in == null) {
      panel.hidden = true;
      return;
    }

    // Antecedent stats — last ~30 days from the Archive API.
    const tiles = [];
    tiles.push(stat(
      `${history.cumulative_precip_in.toFixed(2)}"`,
      `Last ${history.days}d rainfall`,
      `${history.wet_days} wet day${history.wet_days === 1 ? '' : 's'}`
    ));
    if (history.avg_soil_f != null) {
      const trendLabel = history.soil_temp_trend_f == null
        ? ''
        : (history.soil_temp_trend_f >= 0.5 ? `↑ ${history.soil_temp_trend_f.toFixed(1)}°F warming`
          : history.soil_temp_trend_f <= -0.5 ? `↓ ${Math.abs(history.soil_temp_trend_f).toFixed(1)}°F cooling`
          : 'Steady');
      tiles.push(stat(`${history.avg_soil_f.toFixed(0)}°F`, `Avg subsoil temp`, trendLabel));
    }
    if (history.frost_days > 0) {
      tiles.push(stat(`${history.frost_days}`, `Frost days in last ${history.days}d`, ''));
    }

    // Climatology — same-week-of-year averaged across prior years.
    if (climatology && climatology.years_sampled > 0) {
      const days = climatology.days || [];
      const validHi = days.map(d => d.normal_tmax_f).filter(x => x != null);
      const validLo = days.map(d => d.normal_tmin_f).filter(x => x != null);
      if (validHi.length && validLo.length) {
        const avgHi = validHi.reduce((a, b) => a + b, 0) / validHi.length;
        const avgLo = validLo.reduce((a, b) => a + b, 0) / validLo.length;
        const fcastHi = (daily?.tmax_f || []).reduce((a, b) => a + b, 0) / Math.max(1, daily.tmax_f.length);
        const fcastLo = (daily?.tmin_f || []).reduce((a, b) => a + b, 0) / Math.max(1, daily.tmin_f.length);
        const dHi = fcastHi - avgHi;
        const dLo = fcastLo - avgLo;
        const summary = `Forecast week: ${fcastHi.toFixed(0)}° / ${fcastLo.toFixed(0)}°`;
        const dHiSign = dHi >= 0 ? '+' : '';
        const dLoSign = dLo >= 0 ? '+' : '';
        tiles.push(stat(
          `${avgHi.toFixed(0)}° / ${avgLo.toFixed(0)}°`,
          `Typical for this week (${climatology.years_sampled}-yr avg)`,
          `${summary} · ${dHiSign}${dHi.toFixed(0)}° / ${dLoSign}${dLo.toFixed(0)}°`
        ));
      }
      if (climatology.frost_prob_window != null) {
        const pct = Math.round(climatology.frost_prob_window * 100);
        tiles.push(stat(
          `${pct}%`,
          `Historical frost probability`,
          `Avg chance any night below 32°F over the next ${days.length} days`
        ));
      }
    }

    content.innerHTML = tiles.join('');
    panel.hidden = false;
  }

  function stat(value, label, sub) {
    return `
      <div class="hist-tile">
        <div class="hist-value">${value}</div>
        <div class="hist-label">${escapeHTML(label)}</div>
        ${sub ? `<div class="hist-sub muted">${escapeHTML(sub)}</div>` : ''}
      </div>`;
  }

  function drawChart(h) {
    if (typeof Chart === 'undefined') {
      // Chart.js still loading — wait for it.
      return setTimeout(() => drawChart(h), 80);
    }
    const ctx = document.getElementById('hourlyChart').getContext('2d');
    const labels = h.time.map((iso) => {
      const d = new Date(iso);
      return d.toLocaleTimeString([], { weekday: 'short', hour: 'numeric' });
    });
    const css = getComputedStyle(document.documentElement);
    const colorAir = css.getPropertyValue('--accent').trim() || '#c47a2c';
    const colorSoil = css.getPropertyValue('--brand').trim() || '#3f6b3a';
    const colorRain = css.getPropertyValue('--sky').trim() || '#4f7da7';
    const ink = css.getPropertyValue('--ink-2').trim() || '#4a5142';
    const grid = css.getPropertyValue('--line').trim() || '#e3dfd2';

    if (chartInstance) chartInstance.destroy();
    chartInstance = new Chart(ctx, {
      type: 'line',
      data: {
        labels,
        datasets: [
          { label: 'Air °F',  data: h.air_temp_f,  borderColor: colorAir,  backgroundColor: 'transparent', tension: 0.3, yAxisID: 'y' },
          { label: 'Soil °F', data: h.soil_temp_f, borderColor: colorSoil, backgroundColor: 'transparent', tension: 0.3, yAxisID: 'y' },
          { label: 'Rain in', data: h.precip_in,   type: 'bar', backgroundColor: colorRain + '88', borderColor: colorRain, yAxisID: 'y1' },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: 'index', intersect: false },
        scales: {
          x: { ticks: { color: ink, maxTicksLimit: 8 }, grid: { color: grid } },
          y:  { position: 'left',  ticks: { color: ink }, grid: { color: grid }, title: { display: true, text: '°F', color: ink } },
          y1: { position: 'right', ticks: { color: ink }, grid: { drawOnChartArea: false }, title: { display: true, text: 'inches', color: ink } },
        },
        plugins: {
          legend: { labels: { color: ink, usePointStyle: true } },
        },
      },
    });
  }

  function compactPhase(phase) {
    if (!phase) return '—';
    // Shorten maggot phase labels so they fit in narrow cards.
    return phase.replace(/\bmaggot\s+/i, '').replace(/-gen\b/i, 'g').replace(/^~/, '');
  }

  function setupExtendedToggles(data) {
    const horizon = data.forecast_horizon_days || 14;
    const heading = document.getElementById('dailyHeading');
    if (heading) heading.textContent = `${horizon}-day outlook`;

    const sections = [
      { panelId: 'plantPanel', items: data.plant_days || [] },
      { panelId: 'dailyPanel', items: (data.daily && data.daily.is_climate) || [] },
    ];
    const hasExtended = (items) => Array.isArray(items)
      && items.some((x) => x === true || (x && x.is_climate === true));

    sections.forEach((s) => {
      const panel = document.getElementById(s.panelId);
      if (!panel) return;
      const btn = panel.querySelector('.toggle-extended');
      const disclaim = panel.querySelector('.extended-disclaimer');
      if (!btn) return;
      if (!hasExtended(s.items)) {
        btn.hidden = true;
        if (disclaim) disclaim.hidden = true;
        panel.classList.remove('extended');
        return;
      }
      // Reset to collapsed state on each render so a refresh doesn't carry
      // stale extended-state forward into a freshly-loaded report.
      panel.classList.remove('extended');
      btn.hidden = false;
      btn.textContent = 'Show 31 days';
      if (disclaim) disclaim.hidden = true;
      btn.onclick = () => {
        const isOpen = panel.classList.toggle('extended');
        btn.textContent = isOpen ? 'Show 14 days' : 'Show 31 days';
        if (disclaim) disclaim.hidden = !isOpen;
      };
    });
  }

  function renderTacticalCalendar(plantDays, sprayDays) {
    const el = document.getElementById('tacticalCalendar');
    if (!el) return;
    const days = (plantDays || []).filter(d => !d.is_climate).slice(0, 14);
    const sprayMap = {};
    (sprayDays || []).forEach(s => { sprayMap[s.date] = s; });

    el.innerHTML = `
      <div class="cal-legend">
        <span class="cal-legend-item"><span class="cal-dot" data-level="low"></span> Optimal</span>
        <span class="cal-legend-item"><span class="cal-dot" data-level="moderate"></span> Watch</span>
        <span class="cal-legend-item"><span class="cal-dot" data-level="high"></span> Do not plant</span>
        <span class="cal-legend-item"><span class="cal-spray-icon"></span> Spray OK</span>
      </div>
      <div class="cal-strip">
        ${days.map(d => {
          const dt = parseLocalDate(d.date);
          const dow = dt.toLocaleDateString(undefined, { weekday: 'short' });
          const day = dt.getDate();
          const month = dt.toLocaleDateString(undefined, { month: 'short' });
          const level = d.survival_pct >= 90 ? 'low' : d.survival_pct >= 65 ? 'moderate' : 'high';
          const spray = sprayMap[d.date];
          const sprayIcon = spray && spray.verdict === 'GOOD' ? '<span class="cal-spray" title="Spray window available">S</span>' : '';
          return `<div class="cal-day" data-level="${level}" title="${dow} ${month} ${day}: ${d.survival_pct}% survival${spray ? ', spray: ' + spray.verdict : ''}">
            <div class="cal-dow">${dow}</div>
            <div class="cal-date">${day}</div>
            <div class="cal-pct">${d.survival_pct}%</div>
            ${sprayIcon}
          </div>`;
        }).join('')}
      </div>`;
  }

  function renderSprayWindows(sprayDays) {
    const el = document.getElementById('sprayWindows');
    if (!el || !sprayDays || !sprayDays.length) return;
    const days = sprayDays.slice(0, 14);
    el.innerHTML = `
      <h3 class="spray-heading">Spray Windows</h3>
      <p class="muted small">Wind &lt;10mph, no rain within 4h, temp 40–85°F.</p>
      <div class="spray-strip">
        ${days.map(d => {
          const dt = parseLocalDate(d.date);
          const dateLabel = dt.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
          const limiting = (d.limiting_factors || []).join(', ') || 'none';
          return `<div class="spray-day" data-level="${d.level}" title="${dateLabel}: ${d.spray_hours}h available. Limiting: ${limiting}">
            <div class="spray-date">${dateLabel}</div>
            <div class="spray-hrs">${d.spray_hours}h</div>
            <div class="spray-verdict">${d.verdict}</div>
          </div>`;
        }).join('')}
      </div>`;
  }

  function wireIcalExport(data) {
    const btn = document.getElementById('icalExport');
    if (!btn) return;
    const qs = new URLSearchParams(apiQs);
    btn.href = `/api/export/calendar.ics?${qs}`;
  }

  function escapeHTML(s) { return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

  // ----- Field Profitability Dashboard ---------------------------------------

  function fetchAndRenderProfit() {
    const PROFIT_KEY = 'cropsentry_profit_costs';
    let costs = JSON.parse(localStorage.getItem(PROFIT_KEY) || '{}');

    function doFetch() {
      const qs = new URLSearchParams(apiQs);
      qs.set('acres', costs.acres || '80');
      qs.set('seed_cost_ac', costs.seed || '0');
      qs.set('fert_cost_ac', costs.fert || '0');
      qs.set('chem_cost_ac', costs.chem || '0');
      qs.set('fuel_cost_ac', costs.fuel || '0');
      qs.set('other_cost_ac', costs.other || '0');
      qs.set('price_per_bu', costs.price || '0');
      fetch(`/api/profitability?${qs}&_=${Date.now()}`)
        .then(r => r.ok ? r.json() : null)
        .then(data => { if (data) renderProfit(data); })
        .catch(() => {});
    }

    doFetch();

    const editBtn = document.getElementById('profitEditBtn');
    if (editBtn) {
      editBtn.addEventListener('click', () => showProfitModal(costs, (updated) => {
        costs = updated;
        localStorage.setItem(PROFIT_KEY, JSON.stringify(costs));
        doFetch();
      }));
    }
  }

  function showProfitModal(costs, onSave) {
    let modal = document.getElementById('profitModal');
    if (modal) modal.remove();
    modal = document.createElement('div');
    modal.id = 'profitModal';
    modal.className = 'alert-prefs-modal';
    modal.innerHTML = `
      <div class="alert-prefs-dialog fertility-dialog">
        <h3>Input costs & pricing</h3>
        <label>Acres<input type="number" id="pmAcres" min="1" max="50000" value="${costs.acres || 80}"></label>
        <label>Seed ($/ac)<input type="number" id="pmSeed" min="0" step="1" value="${costs.seed || ''}"></label>
        <label>Fertilizer ($/ac)<input type="number" id="pmFert" min="0" step="1" value="${costs.fert || ''}"></label>
        <label>Chemicals ($/ac)<input type="number" id="pmChem" min="0" step="1" value="${costs.chem || ''}"></label>
        <label>Fuel & machinery ($/ac)<input type="number" id="pmFuel" min="0" step="1" value="${costs.fuel || ''}"></label>
        <label>Other ($/ac)<input type="number" id="pmOther" min="0" step="1" value="${costs.other || ''}"></label>
        <label>Market price ($/bu or $/ton)<input type="number" id="pmPrice" min="0" step="0.01" value="${costs.price || ''}"></label>
        <p class="muted small">Leave price blank to use current market defaults.</p>
        <div class="modal-actions">
          <button class="btn primary" id="pmSave">Update</button>
          <button class="btn ghost" id="pmClose">Cancel</button>
        </div>
      </div>`;
    document.body.appendChild(modal);
    modal.addEventListener('click', e => { if (e.target === modal) modal.remove(); });
    document.getElementById('pmClose').addEventListener('click', () => modal.remove());
    document.getElementById('pmSave').addEventListener('click', () => {
      const updated = {
        acres: document.getElementById('pmAcres').value || '80',
        seed: document.getElementById('pmSeed').value || '0',
        fert: document.getElementById('pmFert').value || '0',
        chem: document.getElementById('pmChem').value || '0',
        fuel: document.getElementById('pmFuel').value || '0',
        other: document.getElementById('pmOther').value || '0',
        price: document.getElementById('pmPrice').value || '0',
      };
      modal.remove();
      onSave(updated);
    });
  }

  function renderProfit(data) {
    const panel = document.getElementById('profitPanel');
    if (!panel) return;
    panel.hidden = false;

    const content = document.getElementById('profitContent');
    const profitColor = data.profit_per_acre >= 0 ? '#16a34a' : '#dc2626';
    const costs = data.costs || {};

    let html = '<div class="profit-grid">';

    html += `<div class="profit-card profit-card-main" style="border-color:${profitColor}">
      <h4>Profit / Acre</h4>
      <p class="profit-metric" style="color:${profitColor}">$${data.profit_per_acre.toFixed(2)}</p>
      <p class="muted small">Margin: ${data.margin_pct}%</p>
    </div>`;

    html += `<div class="profit-card">
      <h4>Revenue / Acre</h4>
      <p class="profit-metric">$${data.revenue_per_acre.toFixed(2)}</p>
      <p class="muted small">${data.yield_estimate} ${data.yield_unit} × $${data.price_per_unit}</p>
    </div>`;

    html += `<div class="profit-card">
      <h4>Total Cost / Acre</h4>
      <p class="profit-metric">$${costs.total_per_acre.toFixed(2)}</p>
      <p class="muted small">Break-even: ${data.break_even_yield} ${data.yield_unit}</p>
    </div>`;

    html += `<div class="profit-card">
      <h4>Total Field Profit</h4>
      <p class="profit-metric" style="color:${profitColor}">$${data.total_profit.toLocaleString()}</p>
      <p class="muted small">${data.acres} acres</p>
    </div>`;

    html += '</div>';

    // Cost breakdown
    if (costs.total_per_acre > 0) {
      html += '<div class="profit-breakdown"><h4>Cost Breakdown</h4><div class="profit-bars">';
      const items = [
        { label: 'Seed', val: costs.seed },
        { label: 'Fertilizer', val: costs.fertilizer },
        { label: 'Chemicals', val: costs.chemicals },
        { label: 'Fuel', val: costs.fuel },
        { label: 'Other', val: costs.other },
      ];
      const maxVal = Math.max(...items.map(i => i.val || 0), 1);
      items.forEach(item => {
        if (item.val > 0) {
          const pct = (item.val / maxVal * 100).toFixed(0);
          html += `<div class="profit-bar-row">
            <span class="profit-bar-label">${item.label}</span>
            <div class="profit-bar"><div class="profit-bar-fill" style="width:${pct}%"></div></div>
            <span class="profit-bar-val">$${item.val}/ac</span>
          </div>`;
        }
      });
      html += '</div></div>';
    }

    content.innerHTML = html;
    buildResultsNav();
  }

  // ----- Predictive Yield Estimation ----------------------------------------

  function fetchAndRenderYield() {
    const yieldQs = new URLSearchParams(apiQs);
    fetch(`/api/yield-estimate?${yieldQs}&_=${Date.now()}`)
      .then(r => r.ok ? r.json() : null)
      .then(data => { if (data) renderYield(data); })
      .catch(() => {});
  }

  function renderYield(data) {
    const panel = document.getElementById('yieldPanel');
    if (!panel) return;
    panel.hidden = false;

    const content = document.getElementById('yieldContent');
    const y = data.yield_estimate || {};
    const pct = data.pct_complete || 0;
    const barColor = pct > 75 ? '#16a34a' : pct > 40 ? '#f59e0b' : '#2563eb';

    let html = '<div class="yield-grid">';

    // GDD progress
    html += `<div class="yield-card">
      <h4>GDD Progress</h4>
      <div class="yield-progress-bar"><div class="yield-progress-fill" style="width:${Math.min(pct,100)}%;background:${barColor}"></div></div>
      <p class="yield-metric">${data.current_gdd} / ${data.target_gdd} GDD <span class="muted small">(${pct}%)</span></p>
      <p class="muted small">Pace: ${data.daily_gdd_pace} GDD/day</p>
    </div>`;

    // Maturity estimate
    html += `<div class="yield-card">
      <h4>Estimated Maturity</h4>
      <p class="yield-metric yield-date">${data.estimated_maturity_date || '—'}</p>
      <p class="muted small">${data.days_to_maturity} days remaining</p>
    </div>`;

    // Yield range
    html += `<div class="yield-card yield-range-card">
      <h4>Yield Estimate</h4>
      <div class="yield-range">
        <span class="yield-low">${y.low}</span>
        <span class="yield-expected">${y.expected} <small>${y.unit}</small></span>
        <span class="yield-high">${y.high}</span>
      </div>
      <div class="yield-adjustments muted small">
        NDVI factor: ${data.adjustments?.ndvi_factor || '—'} · Drought factor: ${data.adjustments?.drought_factor || '—'}
      </div>
    </div>`;

    html += '</div>';

    // Contract notes
    if (data.contract_notes && data.contract_notes.length) {
      html += '<div class="yield-notes"><h4>Contract Decision Support</h4>';
      data.contract_notes.forEach(n => {
        html += `<p class="yield-note">${escapeHTML(n)}</p>`;
      });
      html += '</div>';
    }

    html += `<p class="muted small" style="margin-top:12px">Source: ${escapeHTML(data.source || '')}</p>`;
    content.innerHTML = html;
    buildResultsNav();
  }

  // ----- Crop Rotation Intelligence -----------------------------------------

  function fetchAndRenderRotation() {
    const rotQs = new URLSearchParams(apiQs);
    fetch(`/api/rotation?${rotQs}&_=${Date.now()}`)
      .then(r => r.ok ? r.json() : null)
      .then(data => { if (data && data.available) renderRotation(data); })
      .catch(() => {});
  }

  function renderRotation(data) {
    const panel = document.getElementById('rotationPanel');
    if (!panel) return;
    panel.hidden = false;

    const badge = document.getElementById('rotationScore');
    if (badge) {
      const score = data.rotation_score;
      const level = score >= 70 ? 'good' : score >= 40 ? 'fair' : 'poor';
      badge.textContent = `${score}/100`;
      badge.dataset.level = level;
    }

    const content = document.getElementById('rotationContent');
    let html = '';

    // History timeline
    if (data.history && data.history.length) {
      html += '<div class="rotation-timeline"><h4>Rotation History</h4><div class="rotation-years">';
      data.history.forEach(y => {
        const cls = y.crop === data.current_crop ? 'rotation-year same' : 'rotation-year';
        html += `<div class="${cls}"><span class="year-label">${y.year}</span><span class="crop-label">${escapeHTML(y.crop_name || y.crop)}</span></div>`;
      });
      html += `<div class="rotation-year planned"><span class="year-label">${new Date().getFullYear()}</span><span class="crop-label">${escapeHTML(data.current_crop)} (planned)</span></div>`;
      html += '</div></div>';
    }

    // Recommendations
    if (data.recommendations && data.recommendations.length) {
      html += '<div class="rotation-recs">';
      data.recommendations.forEach(r => {
        html += `<div class="rotation-rec" data-type="${r.type}">
          <strong>${escapeHTML(r.title)}</strong>
          <p class="muted small">${escapeHTML(r.detail)}</p>
        </div>`;
      });
      html += '</div>';
    }

    // Cover crop options
    if (data.cover_crop_options && data.cover_crop_options.length) {
      html += '<div class="cover-crops"><h4>Cover Crop Options</h4><div class="cover-crop-grid">';
      data.cover_crop_options.forEach(cc => {
        html += `<div class="cover-crop-card">
          <strong>${escapeHTML(cc.name)}</strong>
          <div class="cover-crop-stats muted small">
            ${cc.n_credit_lb ? `<span>N credit: ${cc.n_credit_lb} lb/ac</span>` : ''}
            <span>Biomass: ${cc.biomass}</span>
            <span>Weed suppression: ${cc.weed_suppression}</span>
          </div>
          ${cc.notes ? `<p class="muted small">${escapeHTML(cc.notes)}</p>` : ''}
        </div>`;
      });
      html += '</div></div>';
    }

    html += `<p class="muted small" style="margin-top:12px">Source: ${escapeHTML(data.source || '')}</p>`;
    content.innerHTML = html;
    buildResultsNav();
  }

  // ----- Fertility & Pest Management ----------------------------------------

  function fetchAndRenderFertility() {
    const fertQs = new URLSearchParams(apiQs);
    const fertState = { yieldGoal: null, prevCrop: 'corn', soilP: null, soilK: null, ph: null };

    function doFetch() {
      const qs = new URLSearchParams(fertQs);
      if (fertState.yieldGoal) qs.set('yield_goal', fertState.yieldGoal);
      if (fertState.prevCrop) qs.set('previous_crop', fertState.prevCrop);
      if (fertState.soilP) qs.set('soil_test_p', fertState.soilP);
      if (fertState.soilK) qs.set('soil_test_k', fertState.soilK);
      if (fertState.ph) qs.set('soil_ph', fertState.ph);

      Promise.all([
        fetch(`/api/fertility?${qs}&_=${Date.now()}`).then(r => r.ok ? r.json() : null),
        fetch(`/api/pest-management?${fertQs}&_=${Date.now()}`).then(r => r.ok ? r.json() : null),
      ]).then(([fert, pest]) => renderFertility(fert, pest, fertState))
        .catch(() => {});
    }

    doFetch();

    const settingsBtn = document.getElementById('fertilitySettingsBtn');
    if (settingsBtn) {
      settingsBtn.addEventListener('click', () => {
        showFertilityModal(fertState, doFetch);
      });
    }
  }

  function showFertilityModal(state, onSave) {
    let modal = document.getElementById('fertilityModal');
    if (modal) modal.remove();
    modal = document.createElement('div');
    modal.id = 'fertilityModal';
    modal.className = 'alert-prefs-modal';
    modal.innerHTML = `
      <div class="alert-prefs-dialog fertility-dialog">
        <h3>Adjust fertility inputs</h3>
        <label>Yield goal (bu/ac)<input type="number" id="fmYield" min="50" max="400" value="${state.yieldGoal || ''}"></label>
        <label>Previous crop
          <select id="fmPrevCrop">
            <option value="corn" ${state.prevCrop==='corn'?'selected':''}>Corn</option>
            <option value="soybeans" ${state.prevCrop==='soybeans'?'selected':''}>Soybeans</option>
            <option value="alfalfa" ${state.prevCrop==='alfalfa'?'selected':''}>Alfalfa</option>
            <option value="wheat" ${state.prevCrop==='wheat'?'selected':''}>Wheat</option>
          </select>
        </label>
        <label>Soil test P (ppm)<input type="number" id="fmP" min="0" max="200" step="1" value="${state.soilP || ''}"></label>
        <label>Soil test K (ppm)<input type="number" id="fmK" min="0" max="600" step="1" value="${state.soilK || ''}"></label>
        <label>Soil pH<input type="number" id="fmPH" min="3.5" max="9" step="0.1" value="${state.ph || ''}"></label>
        <div class="modal-actions">
          <button class="btn primary" id="fmSave">Update</button>
          <button class="btn ghost" id="fmClose">Cancel</button>
        </div>
      </div>`;
    document.body.appendChild(modal);
    modal.addEventListener('click', e => { if (e.target === modal) modal.remove(); });
    document.getElementById('fmClose').addEventListener('click', () => modal.remove());
    document.getElementById('fmSave').addEventListener('click', () => {
      state.yieldGoal = document.getElementById('fmYield').value || null;
      state.prevCrop = document.getElementById('fmPrevCrop').value;
      state.soilP = document.getElementById('fmP').value || null;
      state.soilK = document.getElementById('fmK').value || null;
      state.ph = document.getElementById('fmPH').value || null;
      modal.remove();
      onSave();
    });
  }

  function renderFertility(fert, pest, state) {
    const panel = document.getElementById('fertilityPanel');
    if (!panel || (!fert && !pest)) return;
    panel.hidden = false;

    const content = document.getElementById('fertilityContent');
    let html = '';

    if (fert) {
      html += '<div class="fertility-section"><h3>Nutrient Recommendations</h3>';
      html += `<p class="muted small">Yield goal: ${fert.yield_goal_bu} bu/ac · Previous crop: ${escapeHTML(fert.previous_crop || 'corn')}${fert.soil_texture ? ' · ' + escapeHTML(fert.soil_texture) : ''}</p>`;

      const nutrients = [
        { label: 'Nitrogen', data: fert.nitrogen },
        { label: 'Phosphorus (P₂O₅)', data: fert.phosphorus },
        { label: 'Potassium (K₂O)', data: fert.potassium },
        { label: 'Sulfur', data: fert.sulfur },
      ];
      html += '<div class="nutrient-cards">';
      nutrients.forEach(n => {
        const rate = n.data.recommended_lb_ac || 0;
        const urgency = rate > 100 ? 'high' : rate > 30 ? 'medium' : 'low';
        let detail = '';
        if (n.data.notes && n.data.notes.length) detail = n.data.notes.map(escapeHTML).join('; ');
        else if (n.data.note) detail = escapeHTML(n.data.note);
        else if (n.data.soil_test_category) detail = `Soil test: ${escapeHTML(n.data.soil_test_category)}`;
        html += `<div class="nutrient-card" data-urgency="${urgency}">
          <div class="nutrient-card-head">
            <strong>${n.label}</strong>
            <span class="nutrient-rate">${rate} lb/ac</span>
          </div>
          ${detail ? `<p class="nutrient-notes muted small">${detail}</p>` : ''}
        </div>`;
      });
      html += '</div>';

      if (fert.timing && fert.timing.length) {
        html += '<h4>Application Timing</h4><div class="pest-windows">';
        fert.timing.forEach(t => {
          html += `<div class="pest-window">
            <div class="pest-window-head"><strong>${escapeHTML(t.product)}</strong><span class="muted small">${escapeHTML(t.rate)}</span></div>
            <p class="muted small">${escapeHTML(t.timing)} · ${escapeHTML(t.method)}</p>
          </div>`;
        });
        html += '</div>';
      }

      if (fert.lime) {
        html += `<div class="lime-rec"><strong>Lime:</strong> ${fert.lime.tons_ac} tons/ac to reach pH ${fert.lime.target_ph}. ${escapeHTML(fert.lime.note || '')}</div>`;
      }
      html += `<p class="muted small fertility-source">Source: ${escapeHTML(fert.source || 'Tri-State Fertility Guide')}</p>`;
      html += '</div>';
    }

    if (pest) {
      html += '<div class="fertility-section"><h3>Pest Management Windows</h3>';
      html += `<p class="muted small">Growth stage: <strong>${escapeHTML(pest.current_stage)}</strong>${pest.cum_gdd ? ` · ${pest.cum_gdd} GDD` : ''}</p>`;

      if (pest.herbicide_windows && pest.herbicide_windows.length) {
        html += '<h4>Herbicide</h4><div class="pest-windows">';
        pest.herbicide_windows.forEach(w => {
          const cls = w.active_now ? 'pest-window active' : 'pest-window';
          html += `<div class="${cls}">
            <div class="pest-window-head">
              <strong>${escapeHTML(w.phase)}</strong>
              ${w.active_now ? '<span class="badge badge-active">Active now</span>' : ''}
              <span class="muted small">${escapeHTML(w.window_gdd)}</span>
            </div>
            <p class="muted small">${(w.products || []).map(escapeHTML).join(', ')}</p>
            ${w.notes ? `<p class="pest-note muted small">${escapeHTML(w.notes)}</p>` : ''}
          </div>`;
        });
        html += '</div>';
      }

      if (pest.insecticide_recs && pest.insecticide_recs.length) {
        html += '<h4>Insecticide</h4><div class="pest-windows">';
        pest.insecticide_recs.forEach(w => {
          html += `<div class="pest-window">
            <div class="pest-window-head"><strong>${escapeHTML(w.pest || '')}</strong><span class="muted small">${escapeHTML(w.window || '')}</span></div>
            <p class="muted small">${escapeHTML(w.treatment || '')}</p>
            ${w.threshold ? `<p class="pest-note muted small">Threshold: ${escapeHTML(w.threshold)}</p>` : ''}
          </div>`;
        });
        html += '</div>';
      }

      if (pest.fungicide_recs && pest.fungicide_recs.length) {
        html += '<h4>Fungicide</h4><div class="pest-windows">';
        pest.fungicide_recs.forEach(w => {
          html += `<div class="pest-window">
            <div class="pest-window-head"><strong>${escapeHTML(w.disease || '')}</strong><span class="muted small">${escapeHTML(w.window || '')}</span></div>
            <p class="muted small">${escapeHTML(w.treatment || '')}</p>
            ${w.threshold ? `<p class="pest-note muted small">Threshold: ${escapeHTML(w.threshold)}</p>` : ''}
          </div>`;
        });
        html += '</div>';
      }

      if (pest.rotation_risk_flags && pest.rotation_risk_flags.length) {
        html += '<div class="rotation-risks"><h4>Rotation Risks</h4><ul>';
        pest.rotation_risk_flags.forEach(f => { html += `<li class="muted small">${escapeHTML(f)}</li>`; });
        html += '</ul></div>';
      }

      if (pest.tank_mix_warnings && pest.tank_mix_warnings.length) {
        html += '<details class="tank-mix-details"><summary class="muted small">Tank mix incompatibilities</summary><ul>';
        pest.tank_mix_warnings.forEach(w => {
          html += `<li class="muted small"><strong>${escapeHTML(w.a)}</strong> + <strong>${escapeHTML(w.b)}</strong> — ${escapeHTML(w.reason)}</li>`;
        });
        html += '</ul></details>';
      }

      html += `<p class="muted small fertility-source">Source: ${escapeHTML(pest.source || '')}</p>`;
      html += '</div>';
    }

    content.innerHTML = html;
    buildResultsNav();
  }

  // ----- Alerts & Notifications ---------------------------------------------

  function fetchAndRenderAlerts() {
    const alertsQs = new URLSearchParams(apiQs);
    fetch(`/api/push/check?${alertsQs}`)
      .then(r => r.ok ? r.json() : null)
      .then(data => { if (data && data.alerts && data.alerts.length) renderAlerts(data.alerts); })
      .catch(() => {});
  }

  function renderAlerts(alerts) {
    const panel = document.getElementById('alertsPanel');
    const list = document.getElementById('alertsList');
    if (!panel || !list || !alerts.length) return;
    panel.hidden = false;

    const urgencyOrder = { critical: 0, warning: 1, watch: 2, info: 3 };
    alerts.sort((a, b) => (urgencyOrder[a.urgency] || 9) - (urgencyOrder[b.urgency] || 9));

    list.innerHTML = alerts.map(a => {
      const icon = {
        frost_alert: '❄️',
        optimal_window: '✅',
        severe_weather: '⚠️',
        crop_stress: '🌾',
        spray_window: '💨',
        soil_temp: '🌡️',
      }[a.type] || '⚠️';
      return `<div class="alert-card" data-urgency="${a.urgency}">
        <span class="alert-icon">${icon}</span>
        <div class="alert-body">
          <strong class="alert-title">${escapeHTML(a.title)}</strong>
          <p class="alert-message">${escapeHTML(a.message)}</p>
        </div>
      </div>`;
    }).join('');

    buildResultsNav();
  }

  function wireNotifyButton(data) {
    const btn = document.getElementById('notifyBtn');
    if (!btn || !window.PlantSafeNotifications) return;
    if (!('PushManager' in window)) return;

    btn.hidden = false;
    btn.addEventListener('click', () => {
      const loc = data.location || {};
      window.PlantSafeNotifications.showPreferences(
        loc.lat, loc.lon, loc.place, data.crop?.key || crop
      );
    });
  }

  // ----- NDVI / Field Health ------------------------------------------------

  let ndviChartInstance = null;

  function fetchAndRenderNDVI() {
    const ndviQs = new URLSearchParams(apiQs);
    fetch(`/api/ndvi?${ndviQs}&_=${Date.now()}`)
      .then(r => r.ok ? r.json() : null)
      .then(data => { if (data && data.available) renderNDVI(data); })
      .catch(() => {});
  }

  function renderNDVI(data) {
    const panel = document.getElementById('ndviPanel');
    if (!panel) return;
    panel.hidden = false;

    // Badge
    const badge = document.getElementById('ndviHealth');
    if (badge) {
      badge.textContent = data.health_label;
      badge.dataset.level = data.health_level;
    }

    // Summary
    const summary = document.getElementById('ndviSummary');
    if (summary) {
      const trendHtml = data.trend_label
        ? `<span class="ndvi-trend" data-trend="${data.trend}">${data.trend_label}</span>`
        : '';
      summary.innerHTML = `
        <div class="ndvi-kpi-row">
          <div class="ndvi-kpi">
            <span class="ndvi-kpi-value">${data.latest_ndvi.toFixed(3)}</span>
            <span class="ndvi-kpi-label">Latest NDVI</span>
          </div>
          <div class="ndvi-kpi">
            <span class="ndvi-kpi-value">${data.latest_evi.toFixed(3)}</span>
            <span class="ndvi-kpi-label">Latest EVI</span>
          </div>
          <div class="ndvi-kpi">
            <span class="ndvi-kpi-value">${data.scenes_usable}/${data.scenes_searched}</span>
            <span class="ndvi-kpi-label">Clear scenes</span>
          </div>
          <div class="ndvi-kpi">
            <span class="ndvi-kpi-value">${data.resolution_m}m</span>
            <span class="ndvi-kpi-label">Resolution</span>
          </div>
        </div>
        <p class="ndvi-season-note muted small">${escapeHTML(data.season_note)} ${trendHtml}</p>
      `;
    }

    // Chart
    const canvas = document.getElementById('ndviChart');
    if (canvas && data.readings && data.readings.length > 1 && typeof Chart !== 'undefined') {
      if (ndviChartInstance) ndviChartInstance.destroy();
      const labels = data.readings.map(r => r.date);
      const ndviVals = data.readings.map(r => r.ndvi);
      const eviVals = data.readings.map(r => r.evi);
      ndviChartInstance = new Chart(canvas, {
        type: 'line',
        data: {
          labels,
          datasets: [
            {
              label: 'NDVI',
              data: ndviVals,
              borderColor: '#22c55e',
              backgroundColor: 'rgba(34,197,94,0.1)',
              fill: true,
              tension: 0.3,
              pointRadius: 4,
            },
            {
              label: 'EVI',
              data: eviVals,
              borderColor: '#3b82f6',
              backgroundColor: 'rgba(59,130,246,0.08)',
              fill: false,
              tension: 0.3,
              pointRadius: 3,
              borderDash: [4, 3],
            },
          ],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { position: 'top' },
            tooltip: {
              callbacks: {
                afterBody(items) {
                  const idx = items[0].dataIndex;
                  const r = data.readings[idx];
                  return r.cloud_cover_pct != null ? `Cloud: ${r.cloud_cover_pct}%` : '';
                },
              },
            },
          },
          scales: {
            y: {
              min: -0.1,
              max: 1.0,
              title: { display: true, text: 'Index value' },
            },
            x: {
              title: { display: true, text: 'Date' },
            },
          },
        },
      });
    }

    // Readings table
    const readingsEl = document.getElementById('ndviReadings');
    if (readingsEl && data.readings) {
      readingsEl.innerHTML = `
        <details class="ndvi-details">
          <summary class="muted small">Show ${data.readings.length} readings</summary>
          <table class="ndvi-table">
            <thead><tr><th>Date</th><th>NDVI</th><th>EVI</th><th>Cloud%</th></tr></thead>
            <tbody>
              ${data.readings.map(r => `<tr>
                <td>${r.date || '—'}</td>
                <td><span class="ndvi-chip" style="--ndvi:${r.ndvi}">${r.ndvi.toFixed(3)}</span></td>
                <td>${r.evi.toFixed(3)}</td>
                <td>${r.cloud_cover_pct != null ? r.cloud_cover_pct + '%' : '—'}</td>
              </tr>`).join('')}
            </tbody>
          </table>
        </details>
        <p class="muted small ndvi-source">Source: ${escapeHTML(data.source)} · ${data.latest_date}</p>
      `;
    }

    buildResultsNav();
  }

  // ----- Community / Cooperative Data Sharing --------------------------------

  function fetchAndRenderCommunity() {
    const qs = new URLSearchParams(apiQs);
    fetch(`/api/community/benchmarks?${qs}&_=${Date.now()}`)
      .then(r => r.ok ? r.json() : null)
      .then(data => { if (data) renderCommunity(data); })
      .catch(() => {});

    const contributeBtn = document.getElementById('communityContributeBtn');
    if (contributeBtn) {
      contributeBtn.addEventListener('click', () => showCommunityModal());
    }
  }

  function renderCommunity(data) {
    const panel = document.getElementById('communityPanel');
    if (!panel) return;
    panel.hidden = false;

    const content = document.getElementById('communityContent');
    const county = data.county_benchmarks || {};
    const comm = data.community || {};

    let html = '<div class="community-grid">';

    // County benchmarks card
    html += `<div class="community-card">
      <h4>County Benchmarks (NASS)</h4>
      <p class="community-metric">${county.avg_yield || '—'} <small>bu/ac avg</small></p>
      <div class="community-range">
        <span>${county.low_yield || '—'} low</span>
        <span>${county.high_yield || '—'} high</span>
      </div>
      <p class="muted small">${escapeHTML(county.source || '')}</p>
    </div>`;

    // Community data card
    html += `<div class="community-card">
      <h4>Community Reports</h4>
      <p class="community-metric">${comm.avg_yield != null ? comm.avg_yield : '—'} <small>bu/ac avg</small></p>
      <p class="community-count">${comm.submission_count || 0} submissions in this area</p>
      ${comm.peer_comparison ? `<p class="community-peer">${escapeHTML(comm.peer_comparison)}</p>` : ''}
    </div>`;

    html += '</div>';

    // Privacy note
    html += `<p class="community-privacy muted small">${escapeHTML(data.privacy_note || '')}</p>`;

    content.innerHTML = html;
    buildResultsNav();
  }

  function showCommunityModal() {
    let modal = document.getElementById('communityModal');
    if (modal) modal.remove();
    modal = document.createElement('div');
    modal.id = 'communityModal';
    modal.className = 'alert-prefs-modal';
    modal.innerHTML = `
      <div class="alert-prefs-dialog fertility-dialog">
        <h3>Contribute your field data</h3>
        <p class="muted small" style="margin-bottom:12px">All data is anonymized to a ~7-mile grid. No personal or exact location is stored.</p>
        <label>Crop
          <select id="cmCrop">
            <option value="corn">Corn</option>
            <option value="soybeans">Soybeans</option>
            <option value="winter_wheat">Winter Wheat</option>
            <option value="spring_wheat">Spring Wheat</option>
            <option value="dry_beans">Dry Beans</option>
            <option value="sugar_beets">Sugar Beets</option>
            <option value="alfalfa">Alfalfa</option>
          </select>
        </label>
        <label>Yield (bu/ac or ton/ac)<input type="number" id="cmYield" min="0" step="0.1" placeholder="e.g. 180"></label>
        <label>Soil texture (optional)
          <select id="cmSoil">
            <option value="">— Unknown —</option>
            <option value="clay">Clay</option>
            <option value="clay loam">Clay Loam</option>
            <option value="loam">Loam</option>
            <option value="sandy loam">Sandy Loam</option>
            <option value="sand">Sand</option>
            <option value="silt loam">Silt Loam</option>
            <option value="silty clay">Silty Clay</option>
          </select>
        </label>
        <label>Planting date (optional)<input type="date" id="cmDate"></label>
        <div class="modal-actions">
          <button class="btn primary" id="cmSubmit">Submit anonymously</button>
          <button class="btn ghost" id="cmClose">Cancel</button>
        </div>
        <p id="cmStatus" class="community-status" hidden></p>
      </div>`;
    document.body.appendChild(modal);
    modal.addEventListener('click', e => { if (e.target === modal) modal.remove(); });
    document.getElementById('cmClose').addEventListener('click', () => modal.remove());

    const cropSelect = document.getElementById('cmCrop');
    if (crop) cropSelect.value = crop;

    document.getElementById('cmSubmit').addEventListener('click', () => {
      const yieldVal = parseFloat(document.getElementById('cmYield').value);
      if (!yieldVal || yieldVal <= 0) {
        const st = document.getElementById('cmStatus');
        st.textContent = 'Please enter a valid yield.';
        st.hidden = false;
        return;
      }
      const payload = {
        lat: parseFloat(apiQs.get('lat')) || 0,
        lon: parseFloat(apiQs.get('lon')) || 0,
        crop: cropSelect.value,
        yield_bu_ac: yieldVal,
        soil_texture: document.getElementById('cmSoil').value || null,
        planting_date: document.getElementById('cmDate').value || null,
      };
      fetch('/api/community/submit', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      }).then(r => r.json())
        .then(resp => {
          const st = document.getElementById('cmStatus');
          st.textContent = resp.message || 'Submitted!';
          st.className = 'community-status community-status--ok';
          st.hidden = false;
          setTimeout(() => { modal.remove(); fetchAndRenderCommunity(); }, 1500);
        })
        .catch(() => {
          const st = document.getElementById('cmStatus');
          st.textContent = 'Submission failed. Try again later.';
          st.hidden = false;
        });
    });
  }

  // ----- Validation loop ---------------------------------------------------

  function fetchAndRenderValidation() {
    const panel = document.getElementById('validationPanel');
    if (!panel) return;

    const sep = crop ? `?crop=${crop}&` : '?';
    fetch(`/api/validation/stats${sep}_=${Date.now()}`)
      .then(r => r.ok ? r.json() : null)
      .then(data => { if (data) renderValidation(data); })
      .catch(() => {});

    const reportBtn = document.getElementById('reportOutcomeBtn');
    if (reportBtn) {
      reportBtn.addEventListener('click', () => showOutcomeModal());
    }
  }

  function renderValidation(data) {
    const panel = document.getElementById('validationPanel');
    if (!panel) return;
    panel.hidden = false;

    const statsEl = document.getElementById('validationStats');
    const nassEl = document.getElementById('validationNass');
    const s = data.stats || {};

    let html = '<div class="validation-grid">';

    html += `<div class="validation-card">
      <h4>Reports collected</h4>
      <p class="validation-metric">${s.total_reports || 0}</p>
      <p class="muted small">${s.with_predictions || 0} linked to predictions</p>
    </div>`;

    if (s.with_predictions > 0) {
      html += `<div class="validation-card">
        <h4>Mean Absolute Error</h4>
        <p class="validation-metric">${s.mean_absolute_error != null ? s.mean_absolute_error + '%' : '—'}</p>
        <p class="muted small">Avg distance between predicted & actual emergence</p>
      </div>`;

      html += `<div class="validation-card">
        <h4>Confidence Interval Coverage</h4>
        <p class="validation-metric">${s.ci_coverage_pct != null ? s.ci_coverage_pct + '%' : '—'}</p>
        <p class="muted small">How often actual falls within predicted range</p>
      </div>`;

      html += `<div class="validation-card">
        <h4>Model Bias</h4>
        <p class="validation-metric validation-bias validation-bias--${(s.bias || 'unknown').replace(/[^a-z-]/g, '')}">${escapeHTML(s.bias || 'unknown')}</p>
        <p class="muted small">Mean error: ${s.mean_error != null ? (s.mean_error > 0 ? '+' : '') + s.mean_error + '%' : '—'}</p>
      </div>`;

      if (s.frost_accuracy_pct != null) {
        html += `<div class="validation-card">
          <h4>Frost Prediction Accuracy</h4>
          <p class="validation-metric">${s.frost_accuracy_pct}%</p>
          <p class="muted small">Correct frost damage predictions</p>
        </div>`;
      }
    } else {
      html += `<div class="validation-card validation-card--cta">
        <h4>Help calibrate our model</h4>
        <p>After planting, come back and report how your crop emerged. Your anonymous data helps Crop Sentry give better predictions to all farmers.</p>
        <button type="button" class="btn primary small" onclick="document.getElementById('reportOutcomeBtn').click()">Report an outcome</button>
      </div>`;
    }

    html += '</div>';
    statsEl.innerHTML = html;

    // NASS benchmarks
    const benchmarks = data.nass_benchmarks || [];
    if (benchmarks.length > 0) {
      let nhtml = '<h4 class="validation-sub-heading">USDA NASS Benchmarks (Michigan)</h4>';
      nhtml += '<table class="validation-table"><thead><tr><th>Week</th><th>Crop</th><th>Planted</th><th>Emerged</th><th>Good+Exc</th></tr></thead><tbody>';
      for (const b of benchmarks.slice(0, 8)) {
        nhtml += `<tr>
          <td>${escapeHTML(b.week_ending || '')}</td>
          <td>${escapeHTML(b.crop || '')}</td>
          <td>${b.pct_planted != null ? b.pct_planted + '%' : '—'}</td>
          <td>${b.pct_emerged != null ? b.pct_emerged + '%' : '—'}</td>
          <td>${b.condition_good_excellent != null ? b.condition_good_excellent + '%' : '—'}</td>
        </tr>`;
      }
      nhtml += '</tbody></table>';
      nassEl.innerHTML = nhtml;
    }

    buildResultsNav();
  }

  function showOutcomeModal() {
    let modal = document.getElementById('outcomeModal');
    if (modal) modal.remove();

    const snapId = latest ? latest.snapshot_id || '' : '';
    const evalLat = latest ? (latest.location || {}).lat || '' : '';
    const evalLon = latest ? (latest.location || {}).lon || '' : '';
    const evalCrop = crop || 'corn';
    const today = new Date().toISOString().slice(0, 10);

    modal = document.createElement('div');
    modal.id = 'outcomeModal';
    modal.className = 'alert-prefs-modal';
    modal.innerHTML = `
      <div class="alert-prefs-dialog outcome-dialog">
        <h3>Report field outcome</h3>
        <p class="muted small" style="margin-bottom:12px">Tell us how your planting actually went. All data is anonymous — only a hashed IP is stored.</p>
        <label>Planting date
          <input type="date" id="omPlantDate" value="${today}" required>
        </label>
        <label>Emergence rate (%)
          <input type="number" id="omEmergence" min="0" max="100" step="1" placeholder="e.g. 85">
          <span class="field-hint muted small">Percentage of seeds that emerged</span>
        </label>
        <label>Days to emerge
          <input type="number" id="omDaysToEmerge" min="1" max="60" step="1" placeholder="e.g. 7">
        </label>
        <label>Stand quality
          <select id="omStandQuality">
            <option value="">— Select —</option>
            <option value="excellent">Excellent (uniform, no gaps)</option>
            <option value="good">Good (minor gaps)</option>
            <option value="fair">Fair (uneven stand)</option>
            <option value="poor">Poor (significant gaps/loss)</option>
          </select>
        </label>
        <label class="tile-toggle">
          <input type="checkbox" id="omFrostDamage">
          <span>Frost damage observed</span>
        </label>
        <label>Disease issues (optional)
          <input type="text" id="omDisease" placeholder="e.g. seedling blight">
        </label>
        <label>Pest issues (optional)
          <input type="text" id="omPest" placeholder="e.g. wireworm">
        </label>
        <label>Notes (optional)
          <textarea id="omNotes" rows="2" placeholder="Any other observations"></textarea>
        </label>
        <div class="modal-actions">
          <button class="btn primary" id="omSubmit">Submit outcome</button>
          <button class="btn ghost" id="omClose">Cancel</button>
        </div>
        <p id="omStatus" class="community-status" hidden></p>
      </div>`;

    document.body.appendChild(modal);
    modal.addEventListener('click', e => { if (e.target === modal) modal.remove(); });
    document.getElementById('omClose').addEventListener('click', () => modal.remove());

    document.getElementById('omSubmit').addEventListener('click', () => {
      const plantDate = document.getElementById('omPlantDate').value;
      if (!plantDate) {
        const st = document.getElementById('omStatus');
        st.textContent = 'Please enter a planting date.';
        st.hidden = false;
        return;
      }
      const payload = {
        snapshot_id: snapId || null,
        lat: parseFloat(evalLat) || 0,
        lon: parseFloat(evalLon) || 0,
        crop: evalCrop,
        planting_date: plantDate,
        emergence_pct: parseFloat(document.getElementById('omEmergence').value) || null,
        days_to_emerge: parseInt(document.getElementById('omDaysToEmerge').value) || null,
        stand_quality: document.getElementById('omStandQuality').value,
        frost_damage: document.getElementById('omFrostDamage').checked,
        disease_issues: document.getElementById('omDisease').value,
        pest_issues: document.getElementById('omPest').value,
        notes: document.getElementById('omNotes').value,
      };
      const submitBtn = document.getElementById('omSubmit');
      submitBtn.disabled = true;
      submitBtn.textContent = 'Submitting...';
      fetch('/api/validation/outcome', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      }).then(r => r.json())
        .then(resp => {
          const st = document.getElementById('omStatus');
          if (resp.ok) {
            st.textContent = resp.message || 'Outcome recorded!';
            st.className = 'community-status community-status--ok';
            st.hidden = false;
            setTimeout(() => { modal.remove(); fetchAndRenderValidation(); }, 1500);
          } else {
            st.textContent = resp.error || 'Submission failed.';
            st.hidden = false;
            submitBtn.disabled = false;
            submitBtn.textContent = 'Submit outcome';
          }
        })
        .catch(() => {
          const st = document.getElementById('omStatus');
          st.textContent = 'Submission failed. Try again later.';
          st.hidden = false;
          submitBtn.disabled = false;
          submitBtn.textContent = 'Submit outcome';
        });
    });
  }
})();
