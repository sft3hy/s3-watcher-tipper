// ── multi-domain ──
function showDomainSub(name) {
    document.querySelectorAll('#domain-subtabs .tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.domain-sub').forEach(d => d.style.display = 'none');
    if (event && event.target) {
        event.target.classList.add('active');
    }
    document.getElementById('domain-' + name).style.display = 'block';
}

function renderDomainCharts(containerId, data, chartPrefix) {
    const container = document.getElementById(containerId);
    if (!container) return;
    if (!data || !data._has_data) {
        container.innerHTML = '<div class="card" style="grid-column:1/-1"><div style="color:var(--muted);font-family:var(--font-mono);padding:40px;text-align:center">⊘ No data available for this domain. Data will appear when matching columns are present in ingested datasets.</div></div>';
        return;
    }
    let html = '';
    const skip = ['_has_data', 'unique_vessels', 'unique_aircraft', 'threat_score_hist', 'signal_strength_hist', 'sentiment_breakdown', 'entity_isp_corr'];
    // Summary KPI if available
    if (data.unique_vessels) html += `<div class="card"><div class="card-title">⊞ UNIQUE VESSELS</div><div class="kpi"><div class="value" style="font-size:48px">${data.unique_vessels.toLocaleString()}</div></div></div>`;
    if (data.unique_aircraft) html += `<div class="card"><div class="card-title">⊞ UNIQUE AIRCRAFT</div><div class="kpi"><div class="value" style="font-size:48px">${data.unique_aircraft.toLocaleString()}</div></div></div>`;

    Object.entries(data).forEach(([k, v]) => {
        if (skip.includes(k) || !Array.isArray(v) || !v.length) return;
        html += `<div class="card"><div class="card-title">⊞ ${k.replace(/_/g, ' ').toUpperCase()}</div><canvas id="${chartPrefix}-${k}"></canvas></div>`;
    });

    // Histograms
    if (data.threat_score_hist) html += `<div class="card"><div class="card-title">⊞ THREAT SCORE DISTRIBUTION</div><canvas id="${chartPrefix}-threat"></canvas></div>`;
    if (data.signal_strength_hist) html += `<div class="card"><div class="card-title">⊞ SIGNAL STRENGTH DISTRIBUTION</div><canvas id="${chartPrefix}-signal"></canvas></div>`;
    if (data.sentiment_breakdown) html += `<div class="card"><div class="card-title">⊞ SENTIMENT BREAKDOWN</div><canvas id="${chartPrefix}-sentiment"></canvas></div>`;

    container.innerHTML = html;

    // Render bar charts
    Object.entries(data).forEach(([k, v]) => {
        if (skip.includes(k) || !Array.isArray(v) || !v.length) return;
        makeBar(`${chartPrefix}-${k}`, v.map(d => d.label), v.map(d => d.value), {
            horizontal: true
        });
    });
    if (data.threat_score_hist) makeBar(`${chartPrefix}-threat`, data.threat_score_hist.map(d => d.bin), data.threat_score_hist.map(d => d.count));
    if (data.signal_strength_hist) makeBar(`${chartPrefix}-signal`, data.signal_strength_hist.map(d => d.bin), data.signal_strength_hist.map(d => d.count));
    if (data.sentiment_breakdown) makeBar(`${chartPrefix}-sentiment`, data.sentiment_breakdown.map(d => d.label), data.sentiment_breakdown.map(d => d.value));
}

function renderMultiDomain(maritime, aviation, cyber, rf, osint) {
    renderDomainCharts('maritime-charts', maritime, 'mar');
    renderDomainCharts('aviation-charts', aviation, 'avi');
    renderDomainCharts('cyber-charts', cyber, 'cyb');
    renderDomainCharts('rf-charts', rf, 'rfc');
    renderDomainCharts('osint-charts', osint, 'osi');
}
