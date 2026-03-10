// ── behavioral ──
function renderBehavioral(colocation, cotravel, conetwork) {
    // Co-location table
    const coloc = document.getElementById('colocation-table');
    if (coloc) {
        if (colocation && colocation.length) {
            coloc.innerHTML = `<table class="assoc-table"><thead><tr><th>Entity A</th><th>Entity B</th><th>Co-Events</th></tr></thead><tbody>${colocation.map(r => `<tr><td style="color:var(--glow2)">${r.entity_a || '—'}</td><td style="color:var(--glow2)">${r.entity_b || '—'}</td><td style="color:var(--glow)">${r.co_events}</td></tr>`).join('')}</tbody></table>`;
        } else {
            coloc.innerHTML = '<p style="color:var(--muted);font-family:var(--font-mono);padding:20px;text-align:center">No co-location pairs detected</p>';
        }
    }

    // Co-travel table
    const cotrav = document.getElementById('cotravel-table');
    if (cotrav) {
        if (cotravel && cotravel.length) {
            cotrav.innerHTML = `<table class="assoc-table"><thead><tr><th>Entity A</th><th>Entity B</th><th>Shared Locations</th></tr></thead><tbody>${cotravel.map(r => `<tr><td style="color:var(--glow2)">${r.entity_a || '—'}</td><td style="color:var(--glow2)">${r.entity_b || '—'}</td><td style="color:var(--warn)">${r.shared_locations}</td></tr>`).join('')}</tbody></table>`;
        } else {
            cotrav.innerHTML = '<p style="color:var(--muted);font-family:var(--font-mono);padding:20px;text-align:center">No co-travel pairs detected (need ≥3 shared locations)</p>';
        }
    }

    // Co-network table
    const conet = document.getElementById('conetwork-table');
    if (conet) {
        if (conetwork && conetwork.length) {
            conet.innerHTML = `<table class="assoc-table"><thead><tr><th>Network Type</th><th>Network Name</th><th>Entity Count</th><th>Entities (sample)</th></tr></thead><tbody>${conetwork.map(r => `<tr><td><span class="badge low">${r.network_type}</span></td><td style="color:var(--text)">${r.network_name}</td><td style="color:var(--glow)">${r.entity_count}</td><td style="color:var(--muted);font-size:10px">${(r.entities || []).join(', ')}</td></tr>`).join('')}</tbody></table>`;
        } else {
            conet.innerHTML = '<p style="color:var(--muted);font-family:var(--font-mono);padding:20px;text-align:center">No co-network associations detected</p>';
        }
    }
}
