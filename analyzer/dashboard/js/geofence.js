// ── geofence ──
function renderGeofence(geofence, dwell, geocode) {
    // Zones
    const zones = document.getElementById('geofence-zones');
    if (zones) {
        if (geofence && geofence.zones && geofence.zones.length) {
            zones.innerHTML = `<table class="assoc-table"><thead><tr><th>Zone</th><th>Radius (km)</th><th>Entities</th><th>Events</th></tr></thead><tbody>${geofence.zones.map(z => `<tr><td style="color:var(--warn);font-weight:bold">${z.name}</td><td>${z.radius_km}</td><td style="color:var(--glow)">${z.entities_detected || 0}</td><td>${z.total_events || 0}</td></tr>`).join('')}</tbody></table>`;
        } else {
            zones.innerHTML = '<p style="color:var(--muted);font-family:var(--font-mono);padding:20px;text-align:center">No geofence zones configured</p>';
        }
    }

    // Geofence events
    const evts = document.getElementById('geofence-events');
    if (evts) {
        if (geofence && geofence.events && geofence.events.length) {
            evts.innerHTML = `<table class="assoc-table"><thead><tr><th>Zone</th><th>Entity</th><th>Observations</th><th>Dwell (min)</th></tr></thead><tbody>${geofence.events.map(e => `<tr><td style="color:var(--warn)">${e.zone}</td><td style="color:var(--glow2)">${e.entity_id}</td><td>${e.observations}</td><td style="color:var(--glow)">${e.dwell_minutes}</td></tr>`).join('')}</tbody></table>`;
        } else {
            evts.innerHTML = '<p style="color:var(--muted);font-family:var(--font-mono);padding:20px;text-align:center">No geofence events detected</p>';
        }
    }

    // Dwell sessions
    const dwt = document.getElementById('dwell-table');
    if (dwt) {
        if (dwell && dwell.length) {
            dwt.innerHTML = `<table class="assoc-table"><thead><tr><th>Entity</th><th>Lat</th><th>Lon</th><th>Dwell (min)</th><th>Obs</th></tr></thead><tbody>${dwell.map(d => `<tr><td style="color:var(--glow2)">${d.entity_id}</td><td>${d.lat}</td><td>${d.lon}</td><td style="color:var(--warn);font-weight:bold">${d.dwell_minutes}</td><td style="color:var(--muted)">${d.observations}</td></tr>`).join('')}</tbody></table>`;
        } else {
            dwt.innerHTML = '<p style="color:var(--muted);font-family:var(--font-mono);padding:20px;text-align:center">No dwell sessions detected (need >5 min stationary)</p>';
        }
    }

    // Geocode chart
    if (geocode && geocode.length) {
        makeBar('chart-geocode', geocode.map(g => g.region), geocode.map(g => g.events), {
            horizontal: true
        });
    }
}
