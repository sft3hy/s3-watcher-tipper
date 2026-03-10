// ── SKYTRACE ──
function renderSkytrace(data) {
    const container = document.getElementById('skytrace-charts');
    if (!container) return;
    if (!data || !data._has_data) {
        container.innerHTML = '<div class="card" style="grid-column:1/-1"><div style="color:var(--muted);font-family:var(--font-mono);padding:40px;text-align:center">⊘ No SKYTRACE data available. Data will appear when ISP/satellite connectivity columns are present in ingested datasets.</div></div>';
        return;
    }
    let html = '';
    const skip = ['_has_data', 'entity_isp_corr'];
    Object.entries(data).forEach(([k, v]) => {
        if (skip.includes(k) || !Array.isArray(v) || !v.length) return;
        html += `<div class="card"><div class="card-title">⊞ ${k.replace(/_/g, ' ').toUpperCase()}</div><canvas id="sky-${k}"></canvas></div>`;
    });
    if (data.entity_isp_corr && data.entity_isp_corr.length) {
        html += `<div class="card" style="grid-column:1/-1"><div class="card-title">⊞ ENTITY ↔ ISP CORRELATION</div><div id="sky-corr-table" style="max-height:300px;overflow-y:auto"></div></div>`;
    }
    container.innerHTML = html;
    Object.entries(data).forEach(([k, v]) => {
        if (skip.includes(k) || !Array.isArray(v) || !v.length) return;
        makeBar(`sky-${k}`, v.map(d => d.label), v.map(d => d.value), {
            horizontal: true
        });
    });
    if (data.entity_isp_corr && data.entity_isp_corr.length) {
        document.getElementById('sky-corr-table').innerHTML = `<table class="assoc-table"><thead><tr><th>Entity</th><th>ISP</th><th>Events</th></tr></thead><tbody>${data.entity_isp_corr.map(r => `<tr><td style="color:var(--glow2)">${r.entity_id || '—'}</td><td>${r.isp_name || '—'}</td><td style="color:var(--glow)">${r.count || 0}</td></tr>`).join('')}</tbody></table>`;
    }
}
