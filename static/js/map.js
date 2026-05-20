// Map page: Leaflet + click-to-evaluate + side panel.

(function () {
  function init() {
    if (typeof L === 'undefined') return setTimeout(init, 80);

    delete L.Icon.Default.prototype._getIconUrl;
    L.Icon.Default.mergeOptions({
      iconUrl:       'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png',
      iconRetinaUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png',
      shadowUrl:     'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
    });

    const params = new URLSearchParams(window.location.search);
    const urlLat = parseFloat(params.get('lat'));
    const urlLon = parseFloat(params.get('lon'));
    const hasUrlCoord = Number.isFinite(urlLat) && Number.isFinite(urlLon);
    const startLat = hasUrlCoord ? urlLat : 43.5237;   // Decker, MI
    const startLon = hasUrlCoord ? urlLon : -83.0644;
    const startCrop = params.get('crop') || 'corn';
    if (startCrop === 'soybeans') document.getElementById('mCropSoy').checked = true;

    const map = L.map('map').setView([startLat, startLon], hasUrlCoord ? 11 : 9);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom: 19,
      attribution: '&copy; OpenStreetMap contributors',
    }).addTo(map);

    // Weather radar overlay (IEM NEXRAD composite — free, no auth)
    const radarLayer = L.tileLayer.wms('https://mesonet.agron.iastate.edu/cgi-bin/wms/nexrad/n0q.cgi', {
      layers: 'nexrad-n0q-900913',
      format: 'image/png',
      transparent: true,
      opacity: 0.6,
      attribution: 'NEXRAD via IEM',
    });

    const radarToggle = document.getElementById('radarToggle');
    if (radarToggle) {
      radarToggle.addEventListener('change', () => {
        if (radarToggle.checked) radarLayer.addTo(map);
        else map.removeLayer(radarLayer);
      });
    }


    if (hasUrlCoord) addPin(startLat, startLon, startCrop);

    const pins = [];
    const panel = document.getElementById('mapPanel');
    const emptyPanelHTML = `<p class="muted">Click anywhere on the map to evaluate that point. Drop pins on specific fields to compare.</p>`;
    const clearBtn = document.getElementById('clearPinsBtn');
    clearBtn.addEventListener('click', clearAllPins);

    const PIN_COOLDOWN_MS = 1000;
    let pinInFlight = false;
    let lastVerdictAt = 0;
    map.on('click', (e) => {
      if (pinInFlight) return;
      const remaining = PIN_COOLDOWN_MS - (Date.now() - lastVerdictAt);
      if (remaining > 0) return;
      const crop = document.querySelector('input[name="mcrop"]:checked').value;
      addPin(e.latlng.lat, e.latlng.lng, crop);
    });

    document.getElementById('mapSearch').addEventListener('submit', async (e) => {
      e.preventDefault();
      const q = document.getElementById('mapZip').value.trim();
      if (!q) return;
      const crop = document.querySelector('input[name="mcrop"]:checked').value;
      panel.innerHTML = `<p class="muted">Searching for "${escapeHTML(q)}"…</p>`;
      try {
        const r = await fetch(`/api/evaluate?zip=${encodeURIComponent(q)}&crop=${crop}`);
        if (!r.ok) {
          const body = await r.json().catch(() => ({}));
          throw new Error(body.detail || `Search failed (${r.status})`);
        }
        const data = await r.json();
        map.setView([data.location.lat, data.location.lon], 11);
        renderPanel(data);
        const pin = L.marker([data.location.lat, data.location.lon]).addTo(map);
        bindPinPopup(pin, data);
        pin.openPopup();
        pins.push({ pin, data });
        updateClearBtn();
        if (window.PlantSafe) window.PlantSafe.setLastLocation(data.location.place);
        loadPestPressure(data.location.lat, data.location.lon);
        loadNDVI(data.location.lat, data.location.lon, crop);
      } catch (err) {
        panel.innerHTML = `<p class="error">${escapeHTML(err.message)}</p>`;
      }
    });

    async function addPin(lat, lon, crop) {
      pinInFlight = true;
      const tmpMarker = L.marker([lat, lon], { opacity: 0.7 }).addTo(map);
      tmpMarker.bindPopup(`<em>Evaluating ${lat.toFixed(3)}, ${lon.toFixed(3)}…</em>`, { closeButton: false });
      tmpMarker.openPopup();
      panel.innerHTML = `<p class="muted">Evaluating ${lat.toFixed(3)}, ${lon.toFixed(3)}…</p>`;
      try {
        const r = await fetch(`/api/evaluate?lat=${lat}&lon=${lon}&crop=${crop}`);
        if (!r.ok) {
          const body = await r.json().catch(() => ({}));
          throw new Error(body.detail || `Eval failed (${r.status})`);
        }
        const data = await r.json();
        tmpMarker.setOpacity(1);
        tmpMarker.unbindPopup();
        bindPinPopup(tmpMarker, data);
        tmpMarker.openPopup();
        renderPanel(data);
        pins.push({ pin: tmpMarker, data });
        renderPinList();
        updateClearBtn();
        if (window.PlantSafe) window.PlantSafe.setLastLocation(data.location.place);
        loadPestPressure(data.location.lat, data.location.lon);
        loadNDVI(data.location.lat, data.location.lon, crop);
      } catch (err) {
        map.removeLayer(tmpMarker);
        panel.innerHTML = `<p class="error">${escapeHTML(err.message)}</p>`;
      } finally {
        lastVerdictAt = Date.now();
        pinInFlight = false;
      }
    }

    function renderPanel(data) {
      if (data.outside_us) {
        panel.innerHTML = `
          <h3>${escapeHTML(data.location.place)}</h3>
          <p class="muted">${escapeHTML(data.crop.label)}</p>
          <p class="mini-verdict" data-level="moderate">TBD</p>
          <p>${escapeHTML(data.verdict_detail)}</p>
          <p class="muted small" style="margin-top:8px; padding:8px; background:var(--surface-2,#f5f5f5); border-radius:6px;">⚠ We are currently only calculating survival for locations within the United States of America. International coverage is coming soon.</p>
          <div id="pinListWrap"></div>
        `;
        renderPinList();
        return;
      }
      const order = { low: 0, moderate: 1, high: 2 };
      const level = data.risks.reduce((acc, r) => order[r.level] > order[acc] ? r.level : acc, 'low');
      const params = new URLSearchParams({ lat: data.location.lat, lon: data.location.lon, crop: data.crop.key });
      const survivalPct = data.survival && data.survival.point_pct != null ? data.survival.point_pct : null;
      const survivalText = survivalPct != null ? `${survivalPct}% survival today` : escapeHTML(data.recommendation);
      panel.innerHTML = `
        <h3>${data.location.lat.toFixed(3)}, ${data.location.lon.toFixed(3)}</h3>
        <p class="muted">${escapeHTML(data.crop.label)}</p>
        <p class="mini-verdict" data-level="${level}">${survivalText}</p>
        <div style="margin-top:12px; display:flex; gap:8px; flex-wrap:wrap;">
          <a class="btn primary small" href="/results?${params}">Full report</a>
          <button class="btn ghost small" id="savePin" type="button">Save</button>
          <button class="btn ghost small" id="cluSnapBtn" type="button">Snap to CLU</button>
        </div>
        <div id="pinListWrap"></div>
      `;
      document.getElementById('savePin').addEventListener('click', () => {
        const saveBtn = document.getElementById('savePin');
        const wrap = document.getElementById('saveDetailWrap');
        if (wrap) { wrap.hidden = !wrap.hidden; return; }
        const detail = document.createElement('div');
        detail.id = 'saveDetailWrap';
        detail.className = 'save-detail';
        detail.innerHTML = `
          <label class="field compact"><span class="field-label">Field name</span>
            <input id="saveFieldName" type="text" placeholder="${escapeHTML(data.location.place)}" maxlength="60" /></label>
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
        saveBtn.parentElement.after(detail);
        document.getElementById('confirmSave').addEventListener('click', () => {
          const name = document.getElementById('saveFieldName').value.trim() || data.location.place;
          const status = document.getElementById('saveFieldStatus').value;
          const notes = document.getElementById('saveFieldNotes').value.trim();
          window.PlantSafe.saveField({
            lat: data.location.lat, lon: data.location.lon,
            place: data.location.place, crop: data.crop.key,
            name, status, notes,
          });
          detail.remove();
          saveBtn.textContent = 'Saved';
          saveBtn.disabled = true;
        });
      });
      const cluBtn = document.getElementById('cluSnapBtn');
      if (cluBtn) {
        cluBtn.addEventListener('click', () => {
          cluBtn.textContent = 'Looking up…';
          cluBtn.disabled = true;
          snapToCLU(data.location.lat, data.location.lon).then(feature => {
            if (feature) {
              addCLUBoundary(feature);
              map.fitBounds(drawnItems.getBounds().pad(0.1));
              cluBtn.textContent = 'CLU Added';
            } else {
              cluBtn.textContent = 'No CLU found';
              setTimeout(() => { cluBtn.textContent = 'Snap to CLU'; cluBtn.disabled = false; }, 2000);
            }
          });
        });
      }
      renderPinList();
    }

    function renderPinList() {
      const wrap = document.getElementById('pinListWrap');
      if (!wrap || pins.length < 2) return;
      const order = { low: 0, moderate: 1, high: 2 };
      const items = pins.slice().reverse().map((p, i) => {
        if (p.data.outside_us) {
          return `<li>
            <span>${escapeHTML(p.data.location.place)}</span>
            <strong style="color:var(--warn)">TBD</strong>
          </li>`;
        }
        const level = p.data.risks.reduce((acc, r) => order[r.level] > order[acc] ? r.level : acc, 'low');
        const pct = p.data.survival && p.data.survival.point_pct != null ? p.data.survival.point_pct : null;
        const label = pct != null ? `${pct}%` : escapeHTML(p.data.recommendation);
        return `<li>
          <span>${escapeHTML(p.data.location.place)}</span>
          <strong style="color:var(--${level === 'low' ? 'good' : level === 'moderate' ? 'warn' : 'bad'})">${label}</strong>
        </li>`;
      }).join('');
      wrap.innerHTML = `<ul class="pin-list">${items}</ul>`;
    }

    function popupHTML(data) {
      if (data.outside_us) {
        return `<button class="pin-popup-close" data-pin-delete title="Remove pin" aria-label="Remove pin">×</button>
                <strong>${escapeHTML(data.location.place)}</strong><br>
                <span style="color:#c89028;font-weight:700">TBD</span><br>
                <span class="muted small">USA only</span>`;
      }
      const order = { low: 0, moderate: 1, high: 2 };
      const level = data.risks.reduce((acc, r) => order[r.level] > order[acc] ? r.level : acc, 'low');
      const color = level === 'low' ? '#2e7d4f' : level === 'moderate' ? '#c89028' : '#b03a2e';
      const pct = data.survival && data.survival.point_pct != null ? data.survival.point_pct : null;
      const headline = pct != null ? `${pct}% survival today` : escapeHTML(data.recommendation);
      return `<button class="pin-popup-close" data-pin-delete title="Remove pin" aria-label="Remove pin">×</button>
              <strong>${escapeHTML(data.location.place)}</strong><br>
              <span style="color:${color};font-weight:700">${headline}</span><br>
              ${escapeHTML(data.crop.label)}`;
    }

    function bindPinPopup(pin, data) {
      pin.bindPopup(popupHTML(data), { closeButton: false });
      pin.on('popupopen', (e) => {
        const btn = e.popup.getElement().querySelector('[data-pin-delete]');
        if (btn) btn.onclick = () => removePin(pin);
      });
    }

    function removePin(pin) {
      pin.closePopup();
      map.removeLayer(pin);
      const idx = pins.findIndex(p => p.pin === pin);
      if (idx >= 0) pins.splice(idx, 1);
      if (pins.length === 0) panel.innerHTML = emptyPanelHTML;
      else renderPinList();
      updateClearBtn();
    }

    function clearAllPins() {
      pins.forEach(p => map.removeLayer(p.pin));
      pins.length = 0;
      panel.innerHTML = emptyPanelHTML;
      updateClearBtn();
    }

    function updateClearBtn() {
      clearBtn.hidden = pins.length === 0;
    }

    // ----- NDVI on map -------------------------------------------------------

    function loadNDVI(lat, lon, crop) {
      const panel = document.getElementById('ndviMapPanel');
      const content = document.getElementById('ndviMapContent');
      if (!panel || !content) return;
      panel.hidden = false;
      content.innerHTML = '<p class="muted small">Loading satellite imagery…</p>';
      fetch(`/api/ndvi?lat=${lat}&lon=${lon}&crop=${crop}&_=${Date.now()}`)
        .then(r => r.ok ? r.json() : null)
        .then(data => {
          if (!data || !data.available) {
            content.innerHTML = '<p class="muted small">No satellite imagery available for this location yet.</p>';
            return;
          }
          const trendHtml = data.trend_label
            ? `<span class="ndvi-trend" data-trend="${data.trend}">${escapeHTML(data.trend_label)}</span>`
            : '';
          content.innerHTML = `
            <div style="display:flex; align-items:center; gap:8px; margin-bottom:10px;">
              <span class="ndvi-badge" data-level="${data.health_level}">${escapeHTML(data.health_label)}</span>
              ${trendHtml}
            </div>
            <div class="ndvi-kpi-row" style="grid-template-columns:repeat(2,1fr);">
              <div class="ndvi-kpi">
                <span class="ndvi-kpi-value">${data.latest_ndvi.toFixed(3)}</span>
                <span class="ndvi-kpi-label">NDVI</span>
              </div>
              <div class="ndvi-kpi">
                <span class="ndvi-kpi-value">${data.latest_evi.toFixed(3)}</span>
                <span class="ndvi-kpi-label">EVI</span>
              </div>
            </div>
            <p class="ndvi-season-note muted small" style="margin-top:8px">${escapeHTML(data.season_note)}</p>
            ${data.readings && data.readings.length > 1 ? `
              <details class="ndvi-details">
                <summary class="muted small">${data.readings.length} readings</summary>
                <table class="ndvi-table">
                  <thead><tr><th>Date</th><th>NDVI</th><th>EVI</th></tr></thead>
                  <tbody>${data.readings.slice(0, 8).map(r => `<tr>
                    <td>${r.date || '—'}</td>
                    <td><span class="ndvi-chip" style="--ndvi:${r.ndvi}">${r.ndvi.toFixed(3)}</span></td>
                    <td>${r.evi.toFixed(3)}</td>
                  </tr>`).join('')}</tbody>
                </table>
              </details>` : ''}
            <p class="muted small ndvi-source" style="margin-top:6px">Source: ${escapeHTML(data.source)} · ${escapeHTML(data.latest_date)}</p>
          `;
        })
        .catch(() => {
          content.innerHTML = '<p class="muted small">Could not load field health data.</p>';
        });
    }

    function loadPestPressure(lat, lon) {
      const pestEl = document.getElementById('pestInfo');
      if (!pestEl) return;
      pestEl.innerHTML = '<p class="muted small">Loading pest data…</p>';
      fetch(`/api/pest-pressure?lat=${lat}&lon=${lon}`)
        .then(r => r.ok ? r.json() : Promise.reject())
        .then(data => {
          const pests = data.pests || {};
          const keys = Object.keys(pests);
          if (!keys.length) {
            pestEl.innerHTML = '<p class="muted small">No pest data available for this location.</p>';
            return;
          }
          pestEl.innerHTML = keys.map(k => {
            const p = pests[k];
            const color = p.tier === 'high' ? 'var(--bad)' : p.tier === 'moderate' ? 'var(--warn)' : 'var(--good)';
            return `<div class="pest-row">
              <span class="pest-dot" style="background:${color}"></span>
              <span class="pest-name">${escapeHTML(p.label)}</span>
              <span class="pest-tier" style="color:${color}">${p.tier}</span>
            </div>`;
          }).join('') + `<p class="muted small" style="margin-top:8px">Source: Extension survey aggregates &amp; ISU ICM trap network</p>`;
        })
        .catch(() => {
          pestEl.innerHTML = '<p class="muted small">Could not load pest data.</p>';
        });
    }

    // ----- CLU Snap (USDA FSA Geoportal) -------------------------------------

    function snapToCLU(lat, lon) {
      const url = 'https://gis.sc.egov.usda.gov/arcgis/rest/services/fb/fb_CLU_latest/MapServer/0/query';
      const params = new URLSearchParams({
        geometry: `${lon},${lat}`,
        geometryType: 'esriGeometryPoint',
        spatialRel: 'esriSpatialRelIntersects',
        outFields: 'CLU_ID,Farm_Number,Tract_Number,CLU_Number,CLU_Calculated_Acreage',
        returnGeometry: 'true',
        f: 'geojson',
        inSR: '4326',
        outSR: '4326',
      });
      return fetch(`${url}?${params}`)
        .then(r => r.ok ? r.json() : null)
        .then(data => {
          if (!data || !data.features || !data.features.length) return null;
          return data.features[0];
        })
        .catch(() => null);
    }

    function addCLUBoundary(feature) {
      if (!feature || !feature.geometry) return;
      const props = feature.properties || {};
      const name = props.Farm_Number
        ? `Farm ${props.Farm_Number} / Tract ${props.Tract_Number} / CLU ${props.CLU_Number}`
        : `CLU ${props.CLU_ID || 'Unknown'}`;
      const layer = L.geoJSON(feature, {
        style: { color: '#16a34a', weight: 2, fillOpacity: 0.1, dashArray: '5,5' },
      }).getLayers()[0];
      if (!layer) return;
      layer.feature = {
        type: 'Feature',
        properties: {
          name,
          acres: props.CLU_Calculated_Acreage || calcAcres(layer),
          source: 'USDA CLU',
        },
        geometry: feature.geometry,
      };
      drawnItems.addLayer(layer);
      saveBoundaries();
      renderBoundaryList();
    }

    // ----- Field Boundary Management ----------------------------------------

    const BOUNDARIES_KEY = 'cropsentry_field_boundaries';
    const drawnItems = new L.FeatureGroup();
    map.addLayer(drawnItems);

    if (typeof L.Control.Draw !== 'undefined') {
      const drawControl = new L.Control.Draw({
        edit: { featureGroup: drawnItems },
        draw: {
          polygon: { shapeOptions: { color: '#2563eb', weight: 2 } },
          rectangle: { shapeOptions: { color: '#2563eb', weight: 2 } },
          polyline: false, circle: false, marker: false, circlemarker: false,
        },
      });
      map.addControl(drawControl);

      map.on(L.Draw.Event.CREATED, (e) => {
        const layer = e.layer;
        const name = prompt('Field name:') || `Field ${drawnItems.getLayers().length + 1}`;
        layer.feature = { type: 'Feature', properties: { name, acres: calcAcres(layer) } };
        drawnItems.addLayer(layer);
        saveBoundaries();
        renderBoundaryList();
      });

      map.on(L.Draw.Event.EDITED, () => { saveBoundaries(); renderBoundaryList(); });
      map.on(L.Draw.Event.DELETED, () => { saveBoundaries(); renderBoundaryList(); });
    }

    function calcAcres(layer) {
      const latlngs = layer.getLatLngs()[0];
      if (!latlngs || latlngs.length < 3) return 0;
      let area = 0;
      for (let i = 0; i < latlngs.length; i++) {
        const j = (i + 1) % latlngs.length;
        const xi = latlngs[i].lng * Math.cos(latlngs[i].lat * Math.PI / 180) * 111319.5;
        const yi = latlngs[i].lat * 111319.5;
        const xj = latlngs[j].lng * Math.cos(latlngs[j].lat * Math.PI / 180) * 111319.5;
        const yj = latlngs[j].lat * 111319.5;
        area += xi * yj - xj * yi;
      }
      return Math.round(Math.abs(area / 2) / 4046.86 * 10) / 10;
    }

    function saveBoundaries() {
      const geojson = drawnItems.toGeoJSON();
      localStorage.setItem(BOUNDARIES_KEY, JSON.stringify(geojson));
    }

    function loadBoundaries() {
      const raw = localStorage.getItem(BOUNDARIES_KEY);
      if (!raw) return;
      try {
        const geojson = JSON.parse(raw);
        L.geoJSON(geojson, {
          style: { color: '#2563eb', weight: 2, fillOpacity: 0.1 },
          onEachFeature: (feature, layer) => {
            layer.feature = feature;
            drawnItems.addLayer(layer);
          },
        });
        renderBoundaryList();
      } catch (e) {}
    }

    function renderBoundaryList() {
      const list = document.getElementById('boundaryList');
      if (!list) return;
      const layers = drawnItems.getLayers();
      if (!layers.length) {
        list.innerHTML = '<p class="muted small">No fields defined. Use draw tools to create boundaries.</p>';
        return;
      }
      list.innerHTML = layers.map((l, i) => {
        const props = (l.feature && l.feature.properties) || {};
        const name = props.name || `Field ${i + 1}`;
        const acres = props.acres || calcAcres(l);
        return `<div class="boundary-item">
          <span class="boundary-name">${escapeHTML(name)}</span>
          <span class="boundary-acres muted small">${acres} ac</span>
          <button class="btn-icon boundary-zoom" data-idx="${i}" title="Zoom to field">⊕</button>
          <button class="btn-icon boundary-delete" data-idx="${i}" title="Delete field">×</button>
        </div>`;
      }).join('');

      list.querySelectorAll('.boundary-zoom').forEach(btn => {
        btn.addEventListener('click', () => {
          const layer = layers[parseInt(btn.dataset.idx)];
          if (layer) map.fitBounds(layer.getBounds().pad(0.1));
        });
      });
      list.querySelectorAll('.boundary-delete').forEach(btn => {
        btn.addEventListener('click', () => {
          const layer = layers[parseInt(btn.dataset.idx)];
          if (layer && confirm('Delete this field boundary?')) {
            drawnItems.removeLayer(layer);
            saveBoundaries();
            renderBoundaryList();
          }
        });
      });
    }

    // Import/Export GeoJSON
    const importBtn = document.getElementById('importGeoBtn');
    const exportBtn = document.getElementById('exportGeoBtn');
    const fileInput = document.getElementById('geoFileInput');

    if (importBtn && fileInput) {
      importBtn.addEventListener('click', () => fileInput.click());
      fileInput.addEventListener('change', (e) => {
        const file = e.target.files[0];
        if (!file) return;
        const reader = new FileReader();
        reader.onload = (ev) => {
          try {
            const geojson = JSON.parse(ev.target.result);
            L.geoJSON(geojson, {
              style: { color: '#2563eb', weight: 2, fillOpacity: 0.1 },
              onEachFeature: (feature, layer) => {
                if (!feature.properties.name) {
                  feature.properties.name = `Imported ${drawnItems.getLayers().length + 1}`;
                }
                feature.properties.acres = feature.properties.acres || calcAcres(layer);
                layer.feature = feature;
                drawnItems.addLayer(layer);
              },
            });
            saveBoundaries();
            renderBoundaryList();
            if (drawnItems.getLayers().length) map.fitBounds(drawnItems.getBounds().pad(0.1));
          } catch (err) {
            alert('Invalid GeoJSON file.');
          }
        };
        reader.readAsText(file);
        fileInput.value = '';
      });
    }

    if (exportBtn) {
      exportBtn.addEventListener('click', () => {
        const geojson = drawnItems.toGeoJSON();
        const blob = new Blob([JSON.stringify(geojson, null, 2)], { type: 'application/geo+json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url; a.download = 'cropsentry_fields.geojson';
        a.click();
        URL.revokeObjectURL(url);
      });
    }

    // VRA Script Export
    const vraBtn = document.getElementById('vraExportBtn');
    if (vraBtn) {
      vraBtn.addEventListener('click', async () => {
        const layers = drawnItems.getLayers();
        if (!layers.length) { alert('Draw at least one field boundary first.'); return; }
        const crop = document.querySelector('input[name="mcrop"]:checked').value;
        const geojson = drawnItems.toGeoJSON();
        const boundaries = geojson.features || [];
        vraBtn.textContent = 'Generating…';
        vraBtn.disabled = true;
        try {
          const r = await fetch('/api/vra/generate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ crop, boundaries }),
          });
          if (!r.ok) throw new Error('VRA generation failed');
          const data = await r.json();
          if (data.csv) {
            const blob = new Blob([data.csv], { type: 'text/csv' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url; a.download = 'cropsentry_vra_prescription.csv';
            a.click();
            URL.revokeObjectURL(url);
          }
        } catch (err) {
          alert('Failed to generate VRA prescription.');
        } finally {
          vraBtn.textContent = 'Generate VRA';
          vraBtn.disabled = false;
        }
      });
    }

    loadBoundaries();

    function escapeHTML(s) { return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
  }

  init();
})();
