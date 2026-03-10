// ── API explorer ──
function renderApiExplorer() {
    const endpoints = [{
        method: 'GET',
        path: '/summary',
        desc: 'KPI summary metrics'
    },
    {
        method: 'GET',
        path: '/entities',
        desc: 'Unique entity list with last positions'
    },
    {
        method: 'GET',
        path: '/geo',
        desc: 'Geospatial observation points'
    },
    {
        method: 'GET',
        path: '/timeline',
        desc: 'Daily event timeline with rolling average'
    },
    {
        method: 'GET',
        path: '/behavioral',
        desc: 'Co-location, co-travel, co-network analysis'
    },
    {
        method: 'GET',
        path: '/behavioral/colocation',
        desc: 'Co-located entity pairs'
    },
    {
        method: 'GET',
        path: '/behavioral/cotravel',
        desc: 'Co-traveling entity pairs'
    },
    {
        method: 'GET',
        path: '/behavioral/conetwork',
        desc: 'Shared-network entity groups'
    },
    {
        method: 'GET',
        path: '/geofence',
        desc: 'Geofence zones + dwell analysis'
    },
    {
        method: 'GET',
        path: '/geofence/zones',
        desc: 'Geofence zone detection summary'
    },
    {
        method: 'GET',
        path: '/geofence/dwell',
        desc: 'Dwell session analysis'
    },
    {
        method: 'GET',
        path: '/domains/maritime',
        desc: 'Maritime / AIS / SHADOWFLEET data'
    },
    {
        method: 'GET',
        path: '/domains/aviation',
        desc: 'Aviation / ADS-B data'
    },
    {
        method: 'GET',
        path: '/domains/cyber',
        desc: 'Cyber threat indicators'
    },
    {
        method: 'GET',
        path: '/domains/rf',
        desc: 'RF / SIGINT emitter data'
    },
    {
        method: 'GET',
        path: '/domains/osint',
        desc: 'OSINT source analysis'
    },
    {
        method: 'GET',
        path: '/skytrace',
        desc: 'SKYTRACE satellite ISP correlation'
    },
    {
        method: 'GET',
        path: '/anomalies',
        desc: 'Anomaly detection flags'
    },
    {
        method: 'GET',
        path: '/units',
        desc: 'Military unit breakdowns'
    },
    {
        method: 'GET',
        path: '/devices',
        desc: 'Device brand & platform analysis'
    },
    {
        method: 'GET',
        path: '/movement',
        desc: 'Speed, heading, altitude analysis'
    },
    {
        method: 'GET',
        path: '/geocode',
        desc: 'Geocode region summary'
    },
    {
        method: 'POST',
        path: '/s3/access',
        desc: 'Secure S3 object proxy (body: {bucket, key})'
    },
    ];
    const tbody = document.querySelector('#api-endpoints-table tbody');
    if (!tbody) return;
    tbody.innerHTML = endpoints.map(e => `<tr>
        <td><span class="badge ${e.method === 'POST' ? 'high' : 'low'}">${e.method}</span></td>
        <td style="color:var(--glow);font-family:var(--font-mono)">/s3-watcher/api/v1${e.path}</td>
        <td style="color:var(--muted)">${e.desc}</td>
        <td>${e.method === 'GET' ? `<button onclick="testApi('${e.path}')" style="background:var(--surface);border:1px solid var(--glow);color:var(--glow);padding:4px 12px;cursor:pointer;font-family:var(--font-mono);font-size:10px">TEST</button>` : '—'}</td>
    </tr>`).join('');
}

async function testApi(path) {
    const pre = document.getElementById('api-response');
    if (!pre) return;
    pre.textContent = `Fetching /s3-watcher/api/v1${path} ...`;
    try {
        const res = await fetch(`/s3-watcher/api/v1${path}`);
        const data = await res.json();
        pre.textContent = JSON.stringify(data, null, 2);
    } catch (e) {
        pre.textContent = `Error: ${e.message}`;
    }
}
