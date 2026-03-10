// ── KPI row ──
function renderKPIs(s) {
  const defs = [
    { label: 'TOTAL EVENTS', value: s.total_events.toLocaleString(), sub: 'raw observations' },
    { label: 'UNIQUE ENTITIES', value: s.unique_entities.toLocaleString(), sub: 'tracked entities' },
    { label: 'UNIQUE UNITS', value: s.unique_units.toLocaleString(), sub: 'military units' },
    { label: 'UNIQUE SITES', value: s.unique_sites.toLocaleString(), sub: 'locations' },
    { label: 'TIME SPAN', value: s.time_span_days + 'd', sub: 'coverage window' },
    { label: 'COUNTRIES', value: s.countries || '—', sub: 'distinct country codes' },
    { label: 'GEO EVENTS', value: (s.has_geo || 0).toLocaleString(), sub: 'with coordinates' },
    { label: 'NULL DENSITY', value: (s.null_pct || 0) + '%', sub: 'avg col nulls' },
  ];
  document.getElementById('kpi-row').innerHTML = defs.map(d => `
    <div class="kpi">
      <div class="label">${d.label}</div>
      <div class="value">${d.value}</div>
      <div class="sub">${d.sub}</div>
    </div>`).join('');
}

// ── heatmap ──
function renderHeatmap(data) {
  const grid = document.getElementById('heatmap-grid');
  if (!grid) return;
  const days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
  const maxv = Math.max(...data.map(d => d.count), 1);
  let html = '';
  // hour headers
  html += '<div class="hm-label"></div>';
  for (let h = 0; h < 24; h++) html += `<div class="hm-label">${h}</div>`;
  // rows
  days.forEach(day => {
    html += `<div class="hm-label">${day}</div>`;
    for (let h = 0; h < 24; h++) {
      const cell = data.find(d => d.day === day && d.hour === h);
      const cnt = cell ? cell.count : 0;
      const t = cnt / maxv;
      const r = Math.round(t * 0 + (1 - t) * 7);
      const g = Math.round(t * 255 + (1 - t) * 27);
      const b = Math.round(t * 224 + (1 - t) * 48);
      const a = 0.15 + t * 0.85;
      html += `<div class="hm-cell" style="background:rgba(${r},${g},${b},${a})" title="${day} ${h}:00 — ${cnt} events"></div>`;
    }
  });
  grid.innerHTML = html;
}

// ── associations ──
function renderAssociations(data) {
  const container = document.getElementById('assoc-container');
  if (!container) return;
  if (!data || !data.length) {
    container.innerHTML = '<p style="color:var(--muted); font-family:var(--font-mono); padding:20px; text-align:center">No strong associations detected in this window</p>';
    return;
  }
  const html = `<table class="assoc-table">
    <thead>
      <tr><th>Entity A</th><th>Entity B</th><th>Strength</th></tr>
    </thead>
    <tbody>
      ${data.map(d => `
        <tr>
          <td style="color:var(--glow2)">${d.entity_a}</td>
          <td style="color:var(--glow2)">${d.entity_b}</td>
          <td>
            <div style="display:flex; align-items:center; gap:8px">
              <span style="width:25px; text-align:right">${Math.round(d.score * 100)}%</span>
              <div class="score-bar" style="flex:1"><div class="score-fill" style="width:${d.score * 100}%"></div></div>
            </div>
          </td>
        </tr>`).join('')}
    </tbody>
  </table>`;
  container.innerHTML = html;
}
