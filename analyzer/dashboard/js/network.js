// ── network ──
function renderNetwork(data) {
    const container = document.getElementById('network-charts');
    const keys = ['wifi_ssid', 'connected_wifi_vendor_name', 'carrier'];
    let html = '';
    keys.forEach(k => {
        if (data[k] && data[k].length)
            html += `<div class="card"><div class="card-title">⊞ ${k.replace(/_/g, ' ').toUpperCase()}</div><canvas id="nchart-${k}"></canvas></div>`;
    });
    if (data.ip_enrichment_sample) {
        html += `<div class="card"><div class="card-title">⊞ IP ENRICHMENT SAMPLES</div><div id="ip-samples" style="font-family:var(--font-mono);font-size:11px;color:var(--muted)"></div></div>`;
    }
    container.innerHTML = html;
    keys.forEach(k => {
        if (data[k] && data[k].length)
            makeBar(`nchart-${k}`, data[k].map(d => d.label), data[k].map(d => d.value), {
                horizontal: true
            });
    });
    if (data.ip_enrichment_sample) {
        document.getElementById('ip-samples').innerHTML = data.ip_enrichment_sample.map(s => `<div style="margin:6px 0;padding:6px;border:1px solid #1a2830">${s}</div>`).join('');
    }
}
