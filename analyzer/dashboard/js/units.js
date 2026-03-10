// ── unit charts ──
function renderUnits(data) {
    const container = document.getElementById('units-charts');
    const keys = ['unit_name', 'site_name', 'unit_type', 'unit_echelon'];
    let html = '';
    keys.forEach(k => {
        if (data[k] && data[k].length)
            html += `<div class="card"><div class="card-title">⊞ ${k.replace(/_/g, ' ').toUpperCase()}</div><canvas id="uchart-${k}"></canvas></div>`;
    });
    if (data.score_histograms) {
        Object.keys(data.score_histograms).forEach(k => {
            html += `<div class="card"><div class="card-title">⊞ ${k.toUpperCase()} SCORE DISTRIBUTION</div><canvas id="uchart-score-${k}"></canvas></div>`;
        });
    }
    container.innerHTML = html;
    keys.forEach(k => {
        if (data[k] && data[k].length) {
            const labels = data[k].map(d => d.label);
            const values = data[k].map(d => d.value);
            makeBar(`uchart-${k}`, labels, values, {
                horizontal: true
            });
        }
    });
    if (data.score_histograms) {
        Object.entries(data.score_histograms).forEach(([k, v]) => {
            makeBar(`uchart-score-${k}`, v.map(d => d.bin.toFixed(2)), v.map(d => d.count));
        });
    }
    // echelon on overview
    if (data.unit_echelon) {
        makeBar('chart_echelon', data.unit_echelon.map(d => d.label), data.unit_echelon.map(d => d.value), {
            horizontal: true
        });
    }
}
