import os


def build_html():
    """Assembles the refactorized dashboard HTML from its components."""
    base_dir = os.path.dirname(__file__)
    pages_dir = os.path.join(base_dir, "pages")
    js_dir = os.path.join(base_dir, "js")

    # Read Styles
    with open(os.path.join(base_dir, "styles.css"), "r") as f:
        styles = f.read()

    # Read Pages (in specific order to match layout)
    page_files = [
        "overview.html",
        "timeline.html",
        "geo.html",
        "units.html",
        "devices.html",
        "movement.html",
        "network.html",
        "behavioral.html",
        "geofence.html",
        "multidomain.html",
        "skytrace.html",
        "anomalies.html",
        "api.html",
    ]
    pages_html = ""
    for pf in page_files:
        path = os.path.join(pages_dir, pf)
        if os.path.exists(path):
            with open(path, "r") as f:
                pages_html += f"\n    <!-- {pf.upper()} -->\n"
                pages_html += f.read()

    # Read JS (in dependency order)
    js_files = [
        "globals.js",
        "charts.js",
        "overview.js",
        "timeline.js",
        "geo.js",
        "units.js",
        "devices.js",
        "movement.js",
        "network.js",
        "anomalies.js",
        "behavioral.js",
        "geofence.js",
        "multidomain.js",
        "skytrace.js",
        "api.js",
        "boot.js",
    ]
    js_code = ""
    for jf in js_files:
        path = os.path.join(js_dir, jf)
        if os.path.exists(path):
            with open(path, "r") as f:
                js_code += f"\n        // --- {jf.upper()} ---\n"
                js_code += f.read()

    # Define the template skeleton
    template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SIGINT DASHBOARD</title>
    <link rel="icon" type="image/svg+xml" href="/s3-watcher/favicon.svg">
    <!-- External Dependencies -->
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <style>
{styles}
    </style>
</head>
<body>
    <div id="loading">
        <div class="spinner"></div>
        <p>INITIALIZING...</p>
    </div>

    <div id="topbar">
        <div class="logo">SIGINT DASHBOARD</div>
        <div class="meta" id="data-meta">LOAD DATA...</div>
        <div id="clock"></div>
    </div>

    <div id="tabs">
        <button class="tab active" onclick="showTab('overview')">Overview</button>
        <button class="tab" onclick="showTab('timeline')">Timeline</button>
        <button class="tab" onclick="showTab('geo')">Geospatial</button>
        <button class="tab" onclick="showTab('units')">Units</button>
        <button class="tab" onclick="showTab('devices')">Devices</button>
        <button class="tab" onclick="showTab('movement')">Movement</button>
        <button class="tab" onclick="showTab('network')">Network</button>
        <button class="tab" onclick="showTab('behavioral')">Behavioral</button>
        <button class="tab" onclick="showTab('geofence')">Geofence</button>
        <button class="tab" onclick="showTab('multidomain')">Multi-Domain</button>
        <button class="tab" onclick="showTab('skytrace')">SKYTRACE</button>
        <button class="tab" onclick="showTab('anomalies')">Anomalies</button>
        <button class="tab" onclick="showTab('api')">API</button>
    </div>

{pages_html}

    <script>
{js_code}
    </script>
</body>
</html>"""
    return template
