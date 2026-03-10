// ── device charts ──
function renderDevices(data) {
    const container = document.getElementById('device-charts');
    const keys = ['device_brand', 'platform', 'carrier', 'app_id'];
    let html = '';
    keys.forEach(k => {
        if (data[k] && data[k].length)
            html += `<div class="card"><div class="card-title">⊞ ${k.replace(/_/g, ' ').toUpperCase()}</div><canvas id="dchart-${k}"></canvas></div>`;
    });
    if (data.platform_brand_matrix) {
        html += `<div class="card" style="grid-column:1/-1"><div class="card-title">⊞ PLATFORM × BRAND MATRIX</div><canvas id="dchart-matrix" style="max-height:200px"></canvas></div>`;
    }
    container.innerHTML = html;
    keys.forEach(k => {
        if (data[k] && data[k].length) {
            const labels = data[k].map(d => d.label),
                values = data[k].map(d => d.value);
            if (['platform'].includes(k)) makeDoughnut(`dchart-${k}`, labels, values);
            else makeBar(`dchart-${k}`, labels, values, {
                horizontal: true
            });
        }
    });
    if (data.platform_brand_matrix) {
        const m = data.platform_brand_matrix;
        new Chart(document.getElementById('dchart-matrix'), {
            type: 'bar',
            data: {
                labels: m.platforms,
                datasets: m.brands.map((b, i) => ({
                    label: b,
                    data: m.values.map(row => row[i]),
                    backgroundColor: PALETTE[i % PALETTE.length] + '99',
                    borderColor: PALETTE[i % PALETTE.length],
                    borderWidth: 1,
                }))
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: {
                        labels: {
                            color: MUTED,
                            font: {
                                size: 9
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        stacked: true,
                        grid: {
                            color: DIM
                        },
                        ticks: {
                            color: MUTED
                        }
                    },
                    y: {
                        stacked: true,
                        grid: {
                            color: DIM
                        },
                        ticks: {
                            color: MUTED
                        }
                    }
                }
            }
        });
    }
}
