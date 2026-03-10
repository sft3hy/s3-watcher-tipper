// ── globals ──
let D = null;
const GLOW = '#00ffe0';
const GLOW2 = '#00a8ff';
const WARN = '#ff6b35';
const DIM = '#1a2830';
const MUTED = '#3a5060';

const PALETTE = [
    '#00ffe0', '#00a8ff', '#39ff8a', '#ff6b35', '#ffd23f',
    '#b388ff', '#ff4081', '#18ffff', '#69ff47', '#ff6d00'
];

Chart.defaults.color = '#4a6070';
Chart.defaults.borderColor = '#1a2830';
Chart.defaults.font.family = "'Share Tech Mono', monospace";
Chart.defaults.font.size = 10;

// ── clock ──
function updateClock() {
    const now = new Date();
    const clockEl = document.getElementById('clock');
    if (clockEl) {
        clockEl.textContent = now.toISOString().replace('T', ' ').slice(0, 19) + ' UTC';
    }
}
setInterval(updateClock, 1000);
updateClock();

// ── tabs ──
let _geoRendered = false;
function showTab(name) {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));

    // Find the button that was clicked or the one corresponding to the name
    if (event && event.target && event.target.classList.contains('tab')) {
        event.target.classList.add('active');
    } else {
        const tabs = document.querySelectorAll('.tab');
        for (let t of tabs) {
            if (t.getAttribute('onclick') && t.getAttribute('onclick').includes(`'${name}'`)) {
                t.classList.add('active');
                break;
            }
        }
    }

    document.getElementById('page-' + name).classList.add('active');
    if (name === 'geo' && D && D.geo && !_geoRendered) {
        renderGeo(D.geo);
        _geoRendered = true;
    }
}
