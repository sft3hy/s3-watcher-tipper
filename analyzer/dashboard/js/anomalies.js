// ── anomalies ──
function renderAnomalies(data) {
    const tbody = document.getElementById('anomaly-tbody');
    const colors = {
        critical: GLOW,
        high: WARN,
        medium: '#ffd23f',
        low: '#39ff8a'
    };
    if (!tbody) return;
    tbody.innerHTML = data.map(a => `
    <tr>
      <td ${a.details ? 'style="border-bottom:none"' : ''}><span class="badge ${a.severity}">${a.severity}</span></td>
      <td ${a.details ? 'style="border-bottom:none"' : ''}>${a.type}</td>
      <td style="color:${colors[a.severity] || GLOW};text-shadow:0 0 8px ${colors[a.severity] || GLOW}; ${a.details ? 'border-bottom:none' : ''}">${a.count.toLocaleString()}</td>
      <td style="color:var(--muted); ${a.details ? 'border-bottom:none' : ''}">${a.desc}</td>
    </tr>
    ${a.details && a.details.length > 0 ? `
    <tr>
      <td colspan="4" style="padding: 0 12px 14px 12px;">
        <div style="background:#050a0e; border:1px solid #1a2830; padding: 10px; border-radius: 4px; font-family:var(--font-mono); font-size:11px; max-height: 160px; overflow-y: auto;">
          <div style="color:var(--glow); margin-bottom:6px; font-weight:bold; letter-spacing:1px; text-transform:uppercase;">Top Contributors/Examples:</div>
          <table style="width:100%; border-collapse:collapse;">
            ${a.details.map(d => `<tr><td style="color:var(--text); padding:3px 0; border-bottom:1px solid #0d1a22;">${d.label}</td><td style="color:var(--muted); text-align:right; border-bottom:1px solid #0d1a22;">${d.value.toLocaleString()}</td></tr>`).join('')}
          </table>
        </div>
      </td>
    </tr>
    ` : ''}`).join('') || '<tr><td colspan="4" style="color:var(--ok);padding:20px;text-align:center">✓ No anomalies detected</td></tr>';

    // mini on overview
    const mini = document.getElementById('anomaly-mini');
    if (!mini) return;
    if (!data.length) {
        mini.innerHTML = '<p style="color:var(--ok);font-family:var(--font-mono);padding:20px;text-align:center">✓ CLEAN</p>';
        return;
    }
    mini.innerHTML = data.map(a => `
    <div style="display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-bottom:1px solid var(--border)">
      <span style="font-family:var(--font-mono);font-size:11px">${a.type}</span>
      <div style="text-align:right">
        <span class="badge ${a.severity}" style="margin-right:8px">${a.severity}</span>
        <span style="font-family:var(--font-mono);color:${colors[a.severity] || GLOW}">${a.count.toLocaleString()}</span>
      </div>
    </div>`).join('');
}
