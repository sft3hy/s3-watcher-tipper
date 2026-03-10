// ── movement ──
function renderMovement(data) {
    if (data.speed_hist && data.speed_hist.length) {
        makeBar('chart-speed-hist',
            data.speed_hist.map(d => d.bin),
            data.speed_hist.map(d => d.count));
    }
    if (data.speed_buckets && data.speed_buckets.length) {
        makeBar('chart-speed-buckets',
            data.speed_buckets.map(d => d.label),
            data.speed_buckets.map(d => d.value));
    }
    if (data.altitude_hist && data.altitude_hist.length) {
        makeBar('chart-altitude',
            data.altitude_hist.map(d => d.bin),
            data.altitude_hist.map(d => d.count));
    }
    if (data.compass && data.compass.length) renderCompass(data.compass);
}

// ── compass ──
function renderCompass(data) {
    const svg = document.getElementById('compass-svg');
    const cx = 100,
        cy = 100,
        r = 80;
    const maxv = Math.max(...data.map(d => d.count));
    let paths = '';
    const n = data.length;
    data.forEach((d, i) => {
        const a0 = (i * 360 / n - 90) * Math.PI / 180;
        const a1 = ((i + 1) * 360 / n - 90) * Math.PI / 180;
        const len = maxv > 0 ? (d.count / maxv) * r : 0;
        const x1 = cx + Math.cos(a0) * len,
            y1 = cy + Math.sin(a0) * len;
        const x2 = cx + Math.cos(a1) * len,
            y2 = cy + Math.sin(a1) * len;
        const t = maxv > 0 ? d.count / maxv : 0;
        const rr = Math.round(t * 0 + (1 - t) * 0),
            gg = Math.round(t * 255 + (1 - t) * 100),
            bb = Math.round(t * 224 + (1 - t) * 255);
        paths += `<path d="M${cx},${cy} L${x1.toFixed(1)},${y1.toFixed(1)} A${len.toFixed(1)},${len.toFixed(1)} 0 0,1 ${x2.toFixed(1)},${y2.toFixed(1)} Z" fill="rgba(${rr},${gg},${bb},0.8)"/>`;
    });
    // rings
    let rings = '';
    [0.25, 0.5, 0.75, 1].forEach(s => {
        rings += `<circle cx="${cx}" cy="${cy}" r="${r * s}" fill="none" stroke="#1a2830" stroke-width="1"/>`;
    });
    // cardinal labels
    const labels = [{
        t: 'N',
        a: -90
    }, {
        t: 'E',
        a: 0
    }, {
        t: 'S',
        a: 90
    }, {
        t: 'W',
        a: 180
    }];
    let ltxt = labels.map(l => {
        const a = l.a * Math.PI / 180;
        const x = cx + Math.cos(a) * (r + 12),
            y = cy + Math.sin(a) * (r + 12);
        return `<text x="${x.toFixed(0)}" y="${y.toFixed(0)}" text-anchor="middle" dominant-baseline="middle" fill="#00ffe0" font-size="11" font-family="'Share Tech Mono'">${l.t}</text>`;
    }).join('');
    svg.innerHTML = rings + paths + ltxt;
}
