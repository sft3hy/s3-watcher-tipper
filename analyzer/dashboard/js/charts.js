// ── chart factory ──
function makeBar(id, labels, values, opts = {}) {
    const ctx = document.getElementById(id);
    if (!ctx) return;
    const horizontal = opts.horizontal ?? false;
    return new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                data: values,
                backgroundColor: PALETTE.map(c => c + '99'),
                borderColor: PALETTE,
                borderWidth: 1,
            }]
        },
        options: {
            indexAxis: horizontal ? 'y' : 'x',
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    display: false
                }
            },
            scales: {
                x: {
                    grid: {
                        color: DIM
                    },
                    ticks: {
                        color: MUTED
                    }
                },
                y: {
                    grid: {
                        color: DIM
                    },
                    ticks: {
                        color: MUTED
                    }
                },
            },
            ...opts.extra,
        }
    });
}

function makeLine(id, labels, datasets, opts = {}) {
    const ctx = document.getElementById(id);
    if (!ctx) return;
    return new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    labels: {
                        color: MUTED
                    }
                }
            },
            scales: {
                x: {
                    grid: {
                        color: DIM
                    },
                    ticks: {
                        color: MUTED,
                        maxTicksLimit: 12
                    }
                },
                y: {
                    grid: {
                        color: DIM
                    },
                    ticks: {
                        color: MUTED
                    }
                },
            },
            ...opts.extra,
        }
    });
}

function makeDoughnut(id, labels, values) {
    const ctx = document.getElementById(id);
    if (!ctx) return;
    return new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels,
            datasets: [{
                data: values,
                backgroundColor: PALETTE.map(c => c + 'cc'),
                borderColor: '#070b0f',
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: {
                    position: 'right',
                    labels: {
                        color: MUTED,
                        boxWidth: 10,
                        font: {
                            size: 9
                        }
                    }
                }
            }
        }
    });
}
