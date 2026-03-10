// ── bootstrap ──
async function boot() {
    try {
        const res = await fetch('/s3-watcher/data');
        D = await res.json();
        const loading = document.getElementById('loading');
        if (loading) loading.style.display = 'none';

        const dataMeta = document.getElementById('data-meta');
        if (dataMeta) {
            dataMeta.textContent = `${D.summary.total_events.toLocaleString()} EVENTS · ${D.summary.unique_entities.toLocaleString()} ENTITIES · ${D.summary.time_span_days}d WINDOW`;
        }

        renderKPIs(D.summary);
        renderHeatmap(D.heatmap);
        renderTimeline(D.timeline);
        renderUnits(D.units);
        renderDevices(D.devices);
        renderMovement(D.movement);
        renderNetwork(D.network);
        renderAnomalies(D.anomalies);
        renderAssociations(D.associations);
        renderBehavioral(D.colocation, D.cotravel, D.conetwork);
        renderGeofence(D.geofence, D.dwell, D.geocode);
        renderMultiDomain(D.maritime, D.aviation, D.cyber, D.rf, D.osint);
        renderSkytrace(D.skytrace);
        renderApiExplorer();
    } catch (e) {
        const loadingP = document.getElementById('loading')?.querySelector('p');
        if (loadingP) loadingP.textContent = 'ERROR: ' + e.message;
        console.error('Boot error:', e);
    }
}
boot();
