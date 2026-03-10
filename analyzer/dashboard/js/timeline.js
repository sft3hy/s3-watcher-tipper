// ── timeline ──
function renderTimeline(data) {
    if (!data || !data.length) return;
    const labels = data.map(d => d.date);
    const pRadius = data.length === 1 ? 4 : 0;
    makeLine('chart-timeline', labels, [
        {
            label: 'Events',
            data: data.map(d => d.count),
            borderColor: GLOW2,
            backgroundColor: GLOW2 + '22',
            borderWidth: 1.5,
            fill: true,
            pointRadius: pRadius,
            tension: 0.3,
        },
        {
            label: '7-Day Avg',
            data: data.map(d => d.rolling7),
            borderColor: WARN,
            backgroundColor: 'transparent',
            borderWidth: 2,
            pointRadius: 0,
            tension: 0.4,
            borderDash: [6, 3],
        }
    ]);
}
