// ── geo scatter ──
function renderGeo(points) {
  const wrap = document.getElementById('geo-canvas-wrap');
  if (!wrap || !points.length) return;

  const lats = points.map(p => p.latitude).filter(v => v != null);
  const lons = points.map(p => p.longitude).filter(v => v != null);
  const minLat = Math.min(...lats),
    maxLat = Math.max(...lats);
  const minLon = Math.min(...lons),
    maxLon = Math.max(...lons);

  // Replace canvas with div for Leaflet
  const mapDiv = document.createElement('div');
  mapDiv.id = 'map-layer';
  mapDiv.style.width = '100%';
  mapDiv.style.height = '100%';
  const oldCanvas = document.getElementById('geo-canvas');
  if (oldCanvas) wrap.replaceChild(mapDiv, oldCanvas);

  const map = L.map('map-layer').fitBounds([
    [minLat, minLon],
    [maxLat, maxLon]
  ]);

  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; OpenStreetMap &copy; CARTO',
    subdomains: 'abcd',
    maxZoom: 20
  }).addTo(map);

  const speeds = points.map(p => p.speed).filter(v => v != null);
  const maxSpeed = Math.max(...speeds, 1);

  points.forEach(p => {
    if (p.latitude == null || p.longitude == null) return;
    const spd = p.speed != null ? p.speed / maxSpeed : 0.5;
    const r = Math.round(spd * 0 + (1 - spd * 0));
    const g = Math.round(spd * 255 + (1 - spd) * 168);
    const b = Math.round(spd * 100 + (1 - spd) * 255);

    const marker = L.circleMarker([p.latitude, p.longitude], {
      radius: 4,
      fillColor: `rgba(${r},${g},${b},0.8)`,
      color: `rgba(${r},${g},${b},1)`,
      weight: 1,
      opacity: 1,
      fillOpacity: 0.8
    }).addTo(map);

    marker.on('click', () => {
      showEntityCard(p);
    });
  });

  const info = document.getElementById('geo-info');
  if (info) {
    info.style.zIndex = '1000';
    info.style.background = 'rgba(5,10,14,0.8)';
    info.style.padding = '4px 8px';
    info.style.border = '1px solid var(--border)';
    info.style.borderRadius = '2px';
    info.textContent = `${points.length.toLocaleString()} points · lat [${minLat.toFixed(3)}, ${maxLat.toFixed(3)}] · lon [${minLon.toFixed(3)}, ${maxLon.toFixed(3)}]`;
  }
}

function showEntityCard(p) {
  const card = document.getElementById('entity-card');
  if (!card) return;

  const id = p.entity_id || 'UNKNOWN_ENTITY';
  const type = p.entity_type || 'SIGNAL_SOURCE';
  const isp = p.isp_name || p.satellite_provider || '—';
  const spd = p.speed != null ? p.speed.toFixed(1) + ' km/h' : '—';
  const acc = p.event_location_accuracy_score != null ? (p.event_location_accuracy_score * 100).toFixed(1) + '%' : '—';
  const carrier = p.carrier || '—';
  const wifi = p.wifi_ssid || '—';
  const brand = p.device_brand || '—';
  const model = p.device_model || '—';
  const os = p.device_os || p.platform || '—';
  const alt = p.altitude != null ? p.altitude.toFixed(1) + 'm' : '—';
  const head = p.heading != null ? p.heading.toFixed(1) + '°' : '—';
  const age = p.entity_age != null ? p.entity_age + 'd' : '—';
  const time = p.event_time ? new Date(p.event_time).toLocaleString() : '—';

  card.innerHTML = `
    <div class="entity-card-header">
      <div class="title">TARGET_IDENTIFIED</div>
      <div class="entity-card-close" onclick="closeEntityCard()">&times;</div>
    </div>
    <div class="entity-card-body">
      <!-- Identity Section -->
      <div class="entity-section">
        <div class="entity-section-title">IDENTITY_DATA</div>
        <div class="entity-id-row">
          <div class="entity-id-label">SERIAL_ID</div>
          <div class="entity-id-value">${id}</div>
        </div>
        <div class="entity-stat-grid">
          <div class="entity-stat">
            <div class="label">CLASS</div>
            <div class="value">${type}</div>
          </div>
          <div class="entity-stat">
            <div class="label">AGE</div>
            <div class="value">${age}</div>
          </div>
        </div>
      </div>

      <!-- Connectivity Section -->
      <div class="entity-section">
        <div class="entity-section-title">CONNECTIVITY_LINK</div>
        <div class="entity-stat-grid">
          <div class="entity-stat">
            <div class="label">PROVIDER</div>
            <div class="value" style="font-size:11px">${isp}</div>
          </div>
          <div class="entity-stat">
            <div class="label">CARRIER</div>
            <div class="value">${carrier}</div>
          </div>
          <div class="entity-stat">
            <div class="label">WIFI_SSID</div>
            <div class="value">${wifi}</div>
          </div>
          <div class="entity-stat">
            <div class="label">LOC_ACC</div>
            <div class="value">${acc}</div>
          </div>
        </div>
      </div>

      <!-- Device Section -->
      <div class="entity-section">
        <div class="entity-section-title">DEVICE_TELEMETRY</div>
        <div class="entity-stat-grid">
          <div class="entity-stat">
            <div class="label">BRAND</div>
            <div class="value">${brand}</div>
          </div>
          <div class="entity-stat">
            <div class="label">MODEL</div>
            <div class="value">${model}</div>
          </div>
          <div class="entity-stat">
            <div class="label">OS_TYPE</div>
            <div class="value">${os}</div>
          </div>
          <div class="entity-stat">
            <div class="label">TIMESTAMP</div>
            <div class="value" style="font-size:10px">${time}</div>
          </div>
        </div>
      </div>

      <!-- Precision Section -->
      <div class="entity-section">
        <div class="entity-section-title">PRECISION_LOC</div>
        <div class="entity-stat-grid">
          <div class="entity-stat">
            <div class="label">ALTITUDE</div>
            <div class="value">${alt}</div>
          </div>
          <div class="entity-stat">
            <div class="label">HEADING</div>
            <div class="value">${head}</div>
          </div>
          <div class="entity-stat">
            <div class="label">VELOCITY</div>
            <div class="value">${spd}</div>
          </div>
          <div class="entity-stat" style="border-left-color:var(--glow2)">
             <div class="label" style="color:var(--glow2)">COORD_SET</div>
             <div class="value" style="font-size:10px; color:var(--glow2)">${p.latitude.toFixed(5)}, ${p.longitude.toFixed(5)}</div>
          </div>
        </div>
      </div>
    </div>
    <div class="entity-card-footer">
      SYSTEM_SCAN_COMPLETED // T:${new Date().toLocaleTimeString()}
    </div>
  `;

  card.style.display = 'flex';
}

function closeEntityCard() {
  const card = document.getElementById('entity-card');
  if (card) card.style.display = 'none';
}
