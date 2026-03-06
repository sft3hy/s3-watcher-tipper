"""
Advanced Entity Intelligence Dashboard
Usage: python analyze_viz.py <path_to_csv>
Opens an interactive dashboard at http://localhost:8765
"""

import sys
import json
import math
import os
import threading
import webbrowser
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

import pandas as pd
import numpy as np
import clickhouse_connect

# ── Data Fusion Engine ────────────────────────────────────────────────────────


def load_and_fuse(path: str) -> dict:
    p = Path(path)
    if p.is_dir():
        csv_files = list(p.rglob("*.csv"))
        print(f"[+] Found {len(csv_files)} CSV files in directory '{path}'")
        dfs = []
        for f in csv_files:
            try:
                dfs.append(pd.read_csv(f, low_memory=False))
            except Exception as e:
                print(f"[-] Error loading {f.name}: {e}")
        if dfs:
            df = pd.concat(dfs, ignore_index=True)
        else:
            df = pd.DataFrame()
    else:
        df = pd.read_csv(path, low_memory=False)

    print(f"[+] Loaded {len(df):,} rows × {len(df.columns)} cols")

    # Parse timestamps
    for col in ["event_time", "first_seen", "last_seen"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    # Numeric coercions
    for col in [
        "latitude",
        "longitude",
        "speed",
        "heading",
        "altitude",
        "horizontal_accuracy",
        "vertical_accuracy",
        "event_location_accuracy_score",
        "device_to_unit_association_score",
        "device_at_unit_site_occupancy_metric",
        "entity_age",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return {
        "summary": fuse_summary(df),
        "timeline": fuse_timeline(df),
        "geo": fuse_geo(df),
        "units": fuse_units(df),
        "devices": fuse_devices(df),
        "movement": fuse_movement(df),
        "heatmap": fuse_heatmap(df),
        "associations": fuse_associations(df),
        "anomalies": fuse_anomalies(df),
        "network": fuse_network(df),
    }


def safe_val(v):
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    return v


def df_to_records(df):
    return [{k: safe_val(v) for k, v in row.items()} for row in df.to_dict("records")]


def fuse_summary(df):
    et = (
        df["event_time"].dropna()
        if "event_time" in df.columns
        else pd.Series(dtype="object")
    )
    return {
        "total_events": int(len(df)),
        "unique_entities": (
            int(df["entity_id"].nunique()) if "entity_id" in df.columns else 0
        ),
        "unique_units": int(df["unit_id"].nunique()) if "unit_id" in df.columns else 0,
        "unique_sites": int(df["site_id"].nunique()) if "site_id" in df.columns else 0,
        "time_span_days": int((et.max() - et.min()).days) if len(et) > 1 else 0,
        "countries": (
            int(df["country_code_1"].nunique()) if "country_code_1" in df.columns else 0
        ),
        "has_geo": int(df["latitude"].notna().sum()) if "latitude" in df.columns else 0,
        "null_pct": round(df.isnull().mean().mean() * 100, 1),
    }


def fuse_timeline(df):
    if "event_time" not in df.columns:
        return []
    et = df["event_time"].dropna()
    if et.empty:
        return []
    daily = et.dt.date.value_counts().sort_index()
    # rolling 7-day average
    vals = daily.values.astype(float)
    roll7 = np.convolve(vals, np.ones(7) / 7, mode="same")
    return [
        {"date": str(d), "count": int(c), "rolling7": round(float(r), 1)}
        for d, c, r in zip(daily.index, vals, roll7)
    ]


def fuse_geo(df):
    if "latitude" not in df.columns:
        return []
    sub = df[
        [
            "latitude",
            "longitude",
            "entity_id",
            "unit_name",
            "speed",
            "event_location_accuracy_score",
        ]
    ].dropna(subset=["latitude", "longitude"])
    if len(sub) > 5000:
        sub = sub.sample(5000, random_state=42)
    return df_to_records(sub)


def fuse_units(df):
    out = {}
    for col in [
        "unit_echelon",
        "unit_domain",
        "unit_type_level_1",
        "unit_type_level_2",
        "regional_command",
        "operational_command",
        "orbat",
    ]:
        if col in df.columns and df[col].notna().any():
            vc = df[col].value_counts().head(15)
            out[col] = [{"label": str(k), "value": int(v)} for k, v in vc.items()]

    # Score distributions
    scores = {}
    for col in [
        "device_to_unit_association_score",
        "device_at_unit_site_occupancy_metric",
    ]:
        if col in df.columns:
            s = df[col].dropna()
            if not s.empty:
                hist, edges = np.histogram(s, bins=20)
                scores[col] = [
                    {"bin": round(float(edges[i]), 3), "count": int(hist[i])}
                    for i in range(len(hist))
                ]
    out["score_histograms"] = scores
    return out


def fuse_devices(df):
    out = {}
    for col in ["device_brand", "platform", "carrier", "app_id"]:
        if col in df.columns and df[col].notna().any():
            vc = df[col].value_counts().head(12)
            out[col] = [{"label": str(k), "value": int(v)} for k, v in vc.items()]

    # Platform × brand matrix
    if "platform" in df.columns and "device_brand" in df.columns:
        ct = pd.crosstab(df["platform"], df["device_brand"]).head(8)
        out["platform_brand_matrix"] = {
            "platforms": list(ct.index.astype(str)),
            "brands": list(ct.columns.astype(str)),
            "values": ct.values.tolist(),
        }
    return out


def fuse_movement(df):
    out = {}
    if "speed" in df.columns:
        speed = df["speed"].dropna()
        bins = [0, 0.5, 2, 8, 20, 50, 120, 300, 1e9]
        labels = [
            "Stationary",
            "Walking",
            "Cycling",
            "Urban",
            "Highway",
            "Fast",
            "Very Fast",
            "Extreme",
        ]
        cats = pd.cut(speed, bins=bins, labels=labels)
        vc = cats.value_counts().reindex(labels).fillna(0)
        out["speed_buckets"] = [
            {"label": l, "value": int(v)} for l, v in zip(vc.index, vc.values)
        ]

        hist, edges = np.histogram(speed.clip(0, 300), bins=40)
        out["speed_hist"] = [
            {"bin": round(float(edges[i]), 1), "count": int(hist[i])}
            for i in range(len(hist))
        ]

    if "heading" in df.columns:
        h = df["heading"].dropna()
        compass_bins = np.arange(0, 361, 22.5)
        hist, _ = np.histogram(h % 360, bins=compass_bins)
        dirs = [
            "N",
            "NNE",
            "NE",
            "ENE",
            "E",
            "ESE",
            "SE",
            "SSE",
            "S",
            "SSW",
            "SW",
            "WSW",
            "W",
            "WNW",
            "NW",
            "NNW",
        ]
        out["compass"] = [
            {"dir": dirs[i], "count": int(hist[i])} for i in range(len(dirs))
        ]

    if "altitude" in df.columns:
        alt = df["altitude"].dropna()
        if not alt.empty:
            hist, edges = np.histogram(alt, bins=30)
            out["altitude_hist"] = [
                {"bin": round(float(edges[i]), 0), "count": int(hist[i])}
                for i in range(len(hist))
            ]
    return out


def fuse_heatmap(df):
    """Hour × DayOfWeek event density"""
    if "event_time" not in df.columns:
        return []
    et = df["event_time"].dropna()
    if et.empty:
        return []
    grid = np.zeros((7, 24), dtype=int)
    for ts in et:
        grid[ts.dayofweek][ts.hour] += 1
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    out = []
    for d in range(7):
        for h in range(24):
            out.append({"day": days[d], "hour": h, "count": int(grid[d][h])})
    return out


def fuse_associations(df):
    """Entity ↔ Unit association strength"""
    if "entity_id" not in df.columns or "unit_name" not in df.columns:
        return []
    col = "device_to_unit_association_score"
    if col not in df.columns:
        return []

    agg = (
        df.dropna(subset=[col, "unit_name", "entity_id"])
        .groupby(["entity_id", "unit_name"])[col]
        .agg(["mean", "count"])
        .reset_index()
        .rename(columns={"mean": "avg_score", "count": "observations"})
        .sort_values("avg_score", ascending=False)
        .head(40)
    )
    return df_to_records(agg)


def fuse_anomalies(df):
    flags = []
    if "speed" in df.columns:
        hi = df[df["speed"] > 250]
        if not hi.empty:
            top = hi.sort_values("speed", ascending=False).head(10)
            details = [
                {
                    "label": f"Entity: {row.get('entity_id', 'Unknown')}",
                    "value": round(float(row["speed"]), 1),
                }
                for _, row in top.iterrows()
            ]
            flags.append(
                {
                    "type": "High Speed (>250)",
                    "count": int(len(hi)),
                    "severity": "high",
                    "desc": "Implausibly fast movement",
                    "details": details,
                }
            )
    if "latitude" in df.columns:
        # Check invalid or NaN coords that are way out of bounds
        bad = df[(df["latitude"].abs() > 90) | (df["longitude"].abs() > 180)]
        if not bad.empty:
            top = bad.head(10)
            details = [
                {
                    "label": f"Entity: {row.get('entity_id', 'Unknown')}",
                    "value": f"{round(float(row['latitude']),4)},{round(float(row['longitude']),4)}",
                }
                for _, row in top.iterrows()
            ]
            flags.append(
                {
                    "type": "Bad Coordinates",
                    "count": int(len(bad)),
                    "severity": "critical",
                    "desc": "Lat/lon out of valid range",
                    "details": details,
                }
            )
    if "event_location_accuracy_score" in df.columns:
        low = df[df["event_location_accuracy_score"] < 0.2]
        if not low.empty:
            top = low.nsmallest(10, "event_location_accuracy_score")
            details = [
                {
                    "label": f"Entity: {row.get('entity_id', 'Unknown')}",
                    "value": round(float(row["event_location_accuracy_score"]), 3),
                }
                for _, row in top.iterrows()
            ]
            flags.append(
                {
                    "type": "Low Accuracy",
                    "count": int(len(low)),
                    "severity": "medium",
                    "desc": "Location accuracy score < 0.2",
                    "details": details,
                }
            )
    if "meta_row_id" in df.columns:
        n = int(df.duplicated("meta_row_id").sum())
        if n:
            dups = df[df.duplicated("meta_row_id", keep=False)]
            vc = dups["meta_row_id"].value_counts().head(10)
            details = [{"label": str(k), "value": int(v)} for k, v in vc.items()]
            flags.append(
                {
                    "type": "Duplicate Row IDs",
                    "count": n,
                    "severity": "medium",
                    "desc": "meta_row_id appears more than once",
                    "details": details,
                }
            )
    if "device_to_unit_association_score" in df.columns:
        low_assoc = df[df["device_to_unit_association_score"] < 0.3]
        if not low_assoc.empty:
            top = low_assoc.nsmallest(10, "device_to_unit_association_score")
            details = [
                {
                    "label": f"Entity: {row.get('entity_id', 'Unknown')} -> {row.get('unit_name', 'Unknown')}",
                    "value": round(float(row["device_to_unit_association_score"]), 3),
                }
                for _, row in top.iterrows()
            ]
            flags.append(
                {
                    "type": "Weak Unit Association",
                    "count": int(len(low_assoc)),
                    "severity": "low",
                    "desc": "Device-to-unit score < 0.3",
                    "details": details,
                }
            )
    if "entity_id" in df.columns and "event_time" in df.columns:
        et = df[["entity_id", "event_time"]].dropna()
        freq = et.groupby("entity_id").size()
        burst = freq[freq > freq.mean() + 3 * freq.std()]
        if not burst.empty:
            details = [
                {"label": str(k), "value": int(v)}
                for k, v in burst.sort_values(ascending=False).head(20).items()
            ]
            flags.append(
                {
                    "type": "Burst Entities",
                    "count": int(len(burst)),
                    "severity": "medium",
                    "desc": f"Entities with unusually high event counts (>{int(freq.mean()+3*freq.std())})",
                    "details": details,
                }
            )
    return flags


def fuse_network(df):
    """WiFi / IP / carrier network breakdown"""
    out = {}
    for col in ["wifi_ssid", "connected_wifi_vendor_name", "carrier"]:
        if col in df.columns and df[col].notna().any():
            vc = df[col].value_counts().head(10)
            out[col] = [{"label": str(k), "value": int(v)} for k, v in vc.items()]

    # ip_enrichment parsed breakdown
    if "ip_enrichment" in df.columns and df["ip_enrichment"].notna().any():
        out["ip_enrichment_sample"] = list(df["ip_enrichment"].dropna().unique()[:5])
    return out


# ── HTTP Server ───────────────────────────────────────────────────────────────

DATA_CACHE = {}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/data":
            body = json.dumps(DATA_CACHE).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
        elif parsed.path == "/" or parsed.path == "/index.html":
            html = build_html()
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(html.encode())
        else:
            self.send_response(404)
            self.end_headers()


# ── HTML Dashboard ────────────────────────────────────────────────────────────


def build_html() -> str:
    return r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>SIGINT · Entity Intelligence Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" crossorigin=""/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" crossorigin=""></script>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Barlow+Condensed:wght@300;400;600;700&family=Barlow:wght@300;400&display=swap');

  :root {
    --bg:       #070b0f;
    --surface:  #0d1318;
    --border:   #1a2830;
    --glow:     #00ffe0;
    --glow2:    #00a8ff;
    --warn:     #ff6b35;
    --crit:     #ff2d55;
    --ok:       #39ff8a;
    --dim:      #3a5060;
    --text:     #c8dde8;
    --muted:    #4a6070;
    --font-mono:'Share Tech Mono', monospace;
    --font-head:'Barlow Condensed', sans-serif;
    --font-body:'Barlow', sans-serif;
  }

  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: var(--font-body);
    font-size: 13px;
    min-height: 100vh;
    overflow-x: hidden;
  }

  /* scanline overlay */
  body::before {
    content: '';
    position: fixed; inset: 0;
    background: repeating-linear-gradient(
      0deg, transparent, transparent 2px,
      rgba(0,255,224,.012) 2px, rgba(0,255,224,.012) 4px
    );
    pointer-events: none;
    z-index: 9999;
  }

  /* top bar */
  #topbar {
    display: flex; align-items: center; justify-content: space-between;
    padding: 12px 28px;
    background: linear-gradient(90deg, #00ffe020 0%, transparent 60%);
    border-bottom: 1px solid var(--glow);
    position: sticky; top: 0; z-index: 100;
  }
  #topbar .logo {
    font-family: var(--font-mono);
    font-size: 18px;
    color: var(--glow);
    text-shadow: 0 0 16px var(--glow);
    letter-spacing: 4px;
  }
  #topbar .meta {
    font-family: var(--font-mono);
    font-size: 11px;
    color: var(--muted);
    letter-spacing: 2px;
  }
  #clock {
    font-family: var(--font-mono);
    font-size: 13px;
    color: var(--glow2);
    text-shadow: 0 0 8px var(--glow2);
  }

  /* tab nav */
  #tabs {
    display: flex; gap: 2px;
    padding: 10px 28px 0;
    border-bottom: 1px solid var(--border);
    background: var(--bg);
    position: sticky; top: 53px; z-index: 99;
  }
  .tab {
    font-family: var(--font-head);
    font-size: 13px; font-weight: 600;
    letter-spacing: 2px;
    padding: 8px 20px;
    border: 1px solid transparent;
    border-bottom: none;
    background: transparent;
    color: var(--muted);
    cursor: pointer;
    text-transform: uppercase;
    transition: all .2s;
  }
  .tab:hover { color: var(--text); border-color: var(--border); }
  .tab.active {
    color: var(--glow);
    border-color: var(--glow);
    background: #00ffe008;
    text-shadow: 0 0 8px var(--glow);
  }

  /* content */
  .page { display: none; padding: 24px 28px 48px; }
  .page.active { display: block; }

  /* summary cards */
  .kpi-row { display: flex; flex-wrap: wrap; gap: 16px; margin-bottom: 28px; }
  .kpi {
    flex: 1; min-width: 130px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-top: 2px solid var(--glow);
    padding: 14px 18px;
    position: relative;
    overflow: hidden;
  }
  .kpi::after {
    content: '';
    position: absolute; inset: 0;
    background: linear-gradient(135deg, #00ffe008 0%, transparent 60%);
    pointer-events: none;
  }
  .kpi .label {
    font-family: var(--font-mono);
    font-size: 9px;
    letter-spacing: 3px;
    color: var(--muted);
    text-transform: uppercase;
    margin-bottom: 6px;
  }
  .kpi .value {
    font-family: var(--font-head);
    font-size: 32px; font-weight: 700;
    color: var(--glow);
    text-shadow: 0 0 20px var(--glow);
    line-height: 1;
  }
  .kpi .sub {
    font-size: 10px; color: var(--muted);
    margin-top: 4px; font-family: var(--font-mono);
  }

  /* chart containers */
  .chart-grid { display: grid; gap: 20px; }
  .chart-grid.cols2 { grid-template-columns: 1fr 1fr; }
  .chart-grid.cols3 { grid-template-columns: 1fr 1fr 1fr; }

  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    padding: 18px;
    position: relative;
  }
  .card::before {
    content: '';
    position: absolute; top: 0; left: 0; right: 0; height: 1px;
    background: linear-gradient(90deg, var(--glow) 0%, transparent 70%);
  }
  .card-title {
    font-family: var(--font-head);
    font-size: 11px; font-weight: 600;
    letter-spacing: 3px;
    color: var(--muted);
    text-transform: uppercase;
    margin-bottom: 14px;
  }
  .card canvas { max-height: 260px; }

  /* heatmap */
  #heatmap-grid {
    display: grid;
    grid-template-columns: 40px repeat(24, 1fr);
    gap: 2px;
    font-family: var(--font-mono);
    font-size: 9px;
  }
  .hm-label { color: var(--muted); display: flex; align-items: center; justify-content: flex-end; padding-right: 6px; }
  .hm-cell {
    height: 26px;
    border-radius: 2px;
    transition: transform .15s;
    cursor: default;
    position: relative;
  }
  .hm-cell:hover { transform: scale(1.3); z-index: 10; }

  /* anomaly table */
  .anomaly-table { width: 100%; border-collapse: collapse; }
  .anomaly-table th {
    font-family: var(--font-mono); font-size: 9px;
    letter-spacing: 3px; color: var(--muted);
    text-transform: uppercase;
    text-align: left; padding: 8px 12px;
    border-bottom: 1px solid var(--border);
  }
  .anomaly-table td {
    padding: 10px 12px;
    border-bottom: 1px solid var(--border);
    font-family: var(--font-mono); font-size: 12px;
  }
  .badge {
    display: inline-block;
    padding: 2px 10px;
    font-size: 10px; font-weight: 600;
    letter-spacing: 1px;
    text-transform: uppercase;
    border-radius: 2px;
  }
  .badge.critical { background: #ff2d5530; color: var(--crit); border: 1px solid var(--crit); }
  .badge.high     { background: #ff6b3530; color: var(--warn); border: 1px solid var(--warn); }
  .badge.medium   { background: #ffd23f30; color: #ffd23f;    border: 1px solid #ffd23f; }
  .badge.low      { background: #39ff8a20; color: var(--ok);  border: 1px solid var(--ok); }

  /* geo scatter */
  #geo-canvas-wrap {
    position: relative;
    background: #050a0e;
    border: 1px solid var(--border);
    overflow: hidden;
  }
  #geo-canvas { display: block; width: 100%; }
  #geo-info {
    position: absolute; bottom: 12px; left: 12px;
    font-family: var(--font-mono); font-size: 10px;
    color: var(--muted);
  }

  /* association table */
  .assoc-table { width: 100%; border-collapse: collapse; }
  .assoc-table th {
    font-family: var(--font-mono); font-size: 9px;
    letter-spacing: 2px; color: var(--muted);
    text-transform: uppercase;
    text-align: left; padding: 6px 10px;
    border-bottom: 1px solid var(--border);
  }
  .assoc-table td { padding: 7px 10px; border-bottom: 1px solid #0d1a22; font-family: var(--font-mono); font-size: 11px; }
  .score-bar { height: 6px; background: var(--border); border-radius: 3px; margin-top: 3px; }
  .score-fill { height: 100%; border-radius: 3px; background: linear-gradient(90deg, var(--glow2), var(--glow)); }

  /* loading */
  #loading {
    position: fixed; inset: 0;
    background: var(--bg);
    display: flex; flex-direction: column;
    align-items: center; justify-content: center;
    z-index: 9998;
    gap: 16px;
  }
  #loading .spinner {
    width: 40px; height: 40px;
    border: 2px solid var(--border);
    border-top-color: var(--glow);
    border-radius: 50%;
    animation: spin 1s linear infinite;
  }
  #loading p { font-family: var(--font-mono); color: var(--glow); letter-spacing: 4px; font-size: 12px; }
  @keyframes spin { to { transform: rotate(360deg); } }

  /* compass */
  #compass-wrap { display: flex; justify-content: center; }
  #compass-svg { width: 200px; height: 200px; }

  /* scrollbar */
  ::-webkit-scrollbar { width: 6px; height: 6px; }
  ::-webkit-scrollbar-track { background: var(--bg); }
  ::-webkit-scrollbar-thumb { background: var(--dim); border-radius: 3px; }

  @media (max-width: 900px) {
    .chart-grid.cols2, .chart-grid.cols3 { grid-template-columns: 1fr; }
  }
</style>
</head>
<body>

<div id="loading">
  <div class="spinner"></div>
  <p>LOADING INTELLIGENCE DATA</p>
</div>

<div id="topbar">
  <div class="logo">◈ SIGINT DASHBOARD</div>
  <div class="meta" id="data-meta">LOADING…</div>
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
  <button class="tab" onclick="showTab('anomalies')">Anomalies</button>
</div>

<!-- OVERVIEW -->
<div id="page-overview" class="page active">
  <div class="kpi-row" id="kpi-row"></div>
  <div class="chart-grid cols2">
    <div class="card">
      <div class="card-title">⊞ Activity Heatmap — Hour × Day of Week</div>
      <div id="heatmap-grid"></div>
    </div>
    <div class="card">
      <div class="card-title">⊞ Top Unit Echelons</div>
      <canvas id="chart-echelon"></canvas>
    </div>
    <div class="card">
      <div class="card-title">⊞ Entity Association Strengths (top 40)</div>
      <div id="assoc-container" style="max-height:280px;overflow-y:auto"></div>
    </div>
    <div class="card">
      <div class="card-title">⊞ Anomaly Summary</div>
      <div id="anomaly-mini"></div>
    </div>
  </div>
</div>

<!-- TIMELINE -->
<div id="page-timeline" class="page">
  <div class="chart-grid">
    <div class="card">
      <div class="card-title">⊞ Daily Event Volume + 7-Day Rolling Average</div>
      <canvas id="chart-timeline" style="max-height:320px"></canvas>
    </div>
  </div>
</div>

<!-- GEO -->
<div id="page-geo" class="page">
  <div id="geo-canvas-wrap" style="height:560px">
    <canvas id="geo-canvas"></canvas>
    <div id="geo-info"></div>
  </div>
</div>

<!-- UNITS -->
<div id="page-units" class="page">
  <div class="chart-grid cols2" id="units-charts"></div>
</div>

<!-- DEVICES -->
<div id="page-devices" class="page">
  <div class="chart-grid cols2" id="device-charts"></div>
</div>

<!-- MOVEMENT -->
<div id="page-movement" class="page">
  <div class="chart-grid cols2">
    <div class="card">
      <div class="card-title">⊞ Speed Distribution (0–300)</div>
      <canvas id="chart-speed-hist"></canvas>
    </div>
    <div class="card">
      <div class="card-title">⊞ Speed Buckets</div>
      <canvas id="chart-speed-buckets"></canvas>
    </div>
    <div class="card">
      <div class="card-title">⊞ Compass — Heading Distribution</div>
      <div id="compass-wrap"><svg id="compass-svg" viewBox="0 0 200 200"></svg></div>
    </div>
    <div class="card">
      <div class="card-title">⊞ Altitude Distribution</div>
      <canvas id="chart-altitude"></canvas>
    </div>
  </div>
</div>

<!-- NETWORK -->
<div id="page-network" class="page">
  <div class="chart-grid cols2" id="network-charts"></div>
</div>

<!-- ANOMALIES -->
<div id="page-anomalies" class="page">
  <div class="card" style="margin-bottom: 20px;">
    <div class="card-title">⊞ About Anomalies</div>
    <div style="color:var(--muted); line-height: 1.6; font-family:var(--font-mono); font-size:11px;">
      <p style="margin-bottom: 6px;"><strong style="color:var(--text)">Burst Entities:</strong> Entities generating an abnormally high number of events (&mu; + 3&sigma; above the mean). Often indicates automated polling, misconfigured hardware, or active tracking beacons.</p>
      <p style="margin-bottom: 6px;"><strong style="color:var(--text)">High Speed / Bad Coordinates:</strong> Implausible physics or errant GPS device hardware.</p>
      <p><strong style="color:var(--text)">Weak Unit Association:</strong> Devices with very low confidence scores mapping them to a military unit/site.</p>
    </div>
  </div>
  <div class="card">
    <div class="card-title">⊞ Anomaly Flags</div>
    <table class="anomaly-table">
      <thead><tr>
        <th>Severity</th><th>Type</th><th>Count</th><th>Description</th>
      </tr></thead>
      <tbody id="anomaly-tbody"></tbody>
    </table>
  </div>
</div>

<script>
// ── globals ──
let D = null;
const GLOW  = '#00ffe0';
const GLOW2 = '#00a8ff';
const WARN  = '#ff6b35';
const DIM   = '#1a2830';
const MUTED = '#3a5060';

const PALETTE = [
  '#00ffe0','#00a8ff','#39ff8a','#ff6b35','#ffd23f',
  '#b388ff','#ff4081','#18ffff','#69ff47','#ff6d00'
];

Chart.defaults.color = '#4a6070';
Chart.defaults.borderColor = '#1a2830';
Chart.defaults.font.family = "'Share Tech Mono', monospace";
Chart.defaults.font.size   = 10;

// ── clock ──
function updateClock() {
  const now = new Date();
  document.getElementById('clock').textContent =
    now.toISOString().replace('T',' ').slice(0,19) + ' UTC';
}
setInterval(updateClock, 1000); updateClock();

// ── tabs ──
let _geoRendered = false;
function showTab(name) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  event.target.classList.add('active');
  document.getElementById('page-'+name).classList.add('active');
  if (name === 'geo' && D && D.geo && !_geoRendered) {
    renderGeo(D.geo);
    _geoRendered = true;
  }
}

// ── chart factory ──
function makeBar(id, labels, values, opts={}) {
  const ctx = document.getElementById(id);
  if (!ctx) return;
  const horizontal = opts.horizontal ?? false;
  return new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        data: values,
        backgroundColor: PALETTE.map(c => c+'99'),
        borderColor: PALETTE,
        borderWidth: 1,
      }]
    },
    options: {
      indexAxis: horizontal ? 'y' : 'x',
      responsive: true, maintainAspectRatio: true,
      plugins: { legend: { display: false } },
      scales: {
        x: { grid: { color: DIM }, ticks: { color: MUTED } },
        y: { grid: { color: DIM }, ticks: { color: MUTED } },
      },
      ...opts.extra,
    }
  });
}

function makeLine(id, labels, datasets, opts={}) {
  const ctx = document.getElementById(id);
  if (!ctx) return;
  return new Chart(ctx, {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { labels: { color: MUTED } } },
      scales: {
        x: { grid: { color: DIM }, ticks: { color: MUTED, maxTicksLimit: 12 } },
        y: { grid: { color: DIM }, ticks: { color: MUTED } },
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
      datasets: [{ data: values, backgroundColor: PALETTE.map(c=>c+'cc'), borderColor: '#070b0f', borderWidth: 2 }]
    },
    options: {
      responsive: true,
      plugins: { legend: { position: 'right', labels: { color: MUTED, boxWidth: 10, font:{size:9} } } }
    }
  });
}

// ── KPI row ──
function renderKPIs(s) {
  const defs = [
    { label: 'TOTAL EVENTS',    value: s.total_events.toLocaleString(),    sub: 'raw observations' },
    { label: 'UNIQUE ENTITIES', value: s.unique_entities.toLocaleString(), sub: 'tracked entities' },
    { label: 'UNIQUE UNITS',    value: s.unique_units.toLocaleString(),    sub: 'military units' },
    { label: 'UNIQUE SITES',    value: s.unique_sites.toLocaleString(),    sub: 'locations' },
    { label: 'TIME SPAN',       value: s.time_span_days + 'd',             sub: 'coverage window' },
    { label: 'COUNTRIES',       value: s.countries,                        sub: 'distinct country codes' },
    { label: 'GEO EVENTS',      value: s.has_geo.toLocaleString(),         sub: 'with coordinates' },
    { label: 'NULL DENSITY',    value: s.null_pct + '%',                   sub: 'avg col nulls' },
  ];
  document.getElementById('kpi-row').innerHTML = defs.map(d => `
    <div class="kpi">
      <div class="label">${d.label}</div>
      <div class="value">${d.value}</div>
      <div class="sub">${d.sub}</div>
    </div>`).join('');
}

// ── heatmap ──
function renderHeatmap(data) {
  const grid  = document.getElementById('heatmap-grid');
  const days  = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];
  const maxv  = Math.max(...data.map(d=>d.count));
  let html = '';
  // hour headers
  html += '<div class="hm-label"></div>';
  for(let h=0;h<24;h++) html += `<div class="hm-label">${h}</div>`;
  // rows
  days.forEach(day => {
    html += `<div class="hm-label">${day}</div>`;
    for(let h=0;h<24;h++) {
      const cell = data.find(d=>d.day===day && d.hour===h);
      const cnt  = cell ? cell.count : 0;
      const t    = maxv > 0 ? cnt/maxv : 0;
      const r    = Math.round(t * 0   + (1-t) * 7);
      const g    = Math.round(t * 255 + (1-t) * 27);
      const b    = Math.round(t * 224 + (1-t) * 48);
      const a    = 0.15 + t * 0.85;
      html += `<div class="hm-cell" style="background:rgba(${r},${g},${b},${a})" title="${day} ${h}:00 — ${cnt} events"></div>`;
    }
  });
  grid.innerHTML = html;
}

// ── timeline ──
function renderTimeline(data) {
  if (!data || !data.length) return;
  const labels = data.map(d => d.date);
  const pRadius = data.length === 1 ? 4 : 0;
  makeLine('chart-timeline', labels, [
    {
      label: 'Events',
      data: data.map(d=>d.count),
      borderColor: GLOW2, backgroundColor: GLOW2+'22',
      borderWidth: 1.5, fill: true, pointRadius: pRadius,
      tension: 0.3,
    },
    {
      label: '7-Day Avg',
      data: data.map(d=>d.rolling7),
      borderColor: WARN, backgroundColor: 'transparent',
      borderWidth: 2, pointRadius: 0,
      tension: 0.4, borderDash: [6,3],
    }
  ]);
}

// ── geo scatter ──
function renderGeo(points) {
  const wrap = document.getElementById('geo-canvas-wrap');
  if (!wrap || !points.length) return;

  const lats = points.map(p=>p.latitude).filter(v=>v!=null);
  const lons = points.map(p=>p.longitude).filter(v=>v!=null);
  const minLat=Math.min(...lats), maxLat=Math.max(...lats);
  const minLon=Math.min(...lons), maxLon=Math.max(...lons);

  // Replace canvas with div for Leaflet
  const mapDiv = document.createElement('div');
  mapDiv.id = 'map-layer';
  mapDiv.style.width = '100%';
  mapDiv.style.height = '100%';
  const oldCanvas = document.getElementById('geo-canvas');
  if (oldCanvas) wrap.replaceChild(mapDiv, oldCanvas);

  const map = L.map('map-layer').fitBounds([
    [minLat, minLon],
    [maxLat, maxLon]
  ]);

  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; OpenStreetMap &copy; CARTO',
    subdomains: 'abcd',
    maxZoom: 20
  }).addTo(map);

  const speeds = points.map(p=>p.speed).filter(v=>v!=null);
  const maxSpeed = Math.max(...speeds, 1);

  points.forEach(p => {
    if (p.latitude==null || p.longitude==null) return;
    const spd   = p.speed != null ? p.speed/maxSpeed : 0.5;
    const r = Math.round(spd*0   + (1-spd)*0);
    const g = Math.round(spd*255 + (1-spd)*168);
    const b = Math.round(spd*100 + (1-spd)*255);
    
    L.circleMarker([p.latitude, p.longitude], {
      radius: 3,
      fillColor: `rgba(${r},${g},${b},0.8)`,
      color: `rgba(${r},${g},${b},1)`,
      weight: 1,
      opacity: 1,
      fillOpacity: 0.8
    }).addTo(map);
  });

  const info = document.getElementById('geo-info');
  if (info) {
    info.style.zIndex = '1000';
    info.style.background = 'rgba(5,10,14,0.8)';
    info.style.padding = '4px 8px';
    info.style.border = '1px solid var(--border)';
    info.style.borderRadius = '2px';
    info.textContent = `${points.length.toLocaleString()} points · lat [${minLat.toFixed(3)}, ${maxLat.toFixed(3)}] · lon [${minLon.toFixed(3)}, ${maxLon.toFixed(3)}]`;
  }
}

// ── unit charts ──
function renderUnits(data) {
  const container = document.getElementById('units-charts');
  const keys = ['unit_echelon','unit_domain','unit_type_level_1','unit_type_level_2',
                'regional_command','operational_command','orbat'];
  let html = '';
  keys.forEach(k => {
    if (data[k] && data[k].length) {
      html += `<div class="card"><div class="card-title">⊞ ${k.replace(/_/g,' ').toUpperCase()}</div><canvas id="uchart-${k}"></canvas></div>`;
    }
  });
  // score histograms
  if (data.score_histograms) {
    Object.entries(data.score_histograms).forEach(([k,v]) => {
      html += `<div class="card"><div class="card-title">⊞ ${k.replace(/_/g,' ').toUpperCase()}</div><canvas id="uchart-score-${k}"></canvas></div>`;
    });
  }
  container.innerHTML = html;
  keys.forEach(k => {
    if (data[k] && data[k].length) {
      const labels = data[k].map(d=>d.label);
      const values = data[k].map(d=>d.value);
      makeBar(`uchart-${k}`, labels, values, {horizontal: true});
    }
  });
  if (data.score_histograms) {
    Object.entries(data.score_histograms).forEach(([k,v]) => {
      makeBar(`uchart-score-${k}`, v.map(d=>d.bin.toFixed(2)), v.map(d=>d.count));
    });
  }
  // echelon on overview
  if (data.unit_echelon) {
    makeBar('chart-echelon', data.unit_echelon.map(d=>d.label), data.unit_echelon.map(d=>d.value), {horizontal:true});
  }
}

// ── device charts ──
function renderDevices(data) {
  const container = document.getElementById('device-charts');
  const keys = ['device_brand','platform','carrier','app_id'];
  let html = '';
  keys.forEach(k => {
    if (data[k] && data[k].length)
      html += `<div class="card"><div class="card-title">⊞ ${k.replace(/_/g,' ').toUpperCase()}</div><canvas id="dchart-${k}"></canvas></div>`;
  });
  if (data.platform_brand_matrix) {
    html += `<div class="card" style="grid-column:1/-1"><div class="card-title">⊞ PLATFORM × BRAND MATRIX</div><canvas id="dchart-matrix" style="max-height:200px"></canvas></div>`;
  }
  container.innerHTML = html;
  keys.forEach(k => {
    if (data[k] && data[k].length) {
      const labels = data[k].map(d=>d.label), values = data[k].map(d=>d.value);
      if (['platform'].includes(k)) makeDoughnut(`dchart-${k}`, labels, values);
      else makeBar(`dchart-${k}`, labels, values, {horizontal:true});
    }
  });
  if (data.platform_brand_matrix) {
    const m = data.platform_brand_matrix;
    new Chart(document.getElementById('dchart-matrix'), {
      type: 'bar',
      data: {
        labels: m.platforms,
        datasets: m.brands.map((b,i) => ({
          label: b, data: m.values.map(row=>row[i]),
          backgroundColor: PALETTE[i%PALETTE.length]+'99',
          borderColor: PALETTE[i%PALETTE.length], borderWidth: 1,
        }))
      },
      options: {
        responsive:true, maintainAspectRatio:true,
        plugins:{ legend:{ labels:{color:MUTED, font:{size:9}} } },
        scales:{
          x:{stacked:true,grid:{color:DIM},ticks:{color:MUTED}},
          y:{stacked:true,grid:{color:DIM},ticks:{color:MUTED}}
        }
      }
    });
  }
}

// ── movement ──
function renderMovement(data) {
  if (data.speed_hist && data.speed_hist.length) {
    makeBar('chart-speed-hist',
      data.speed_hist.map(d=>d.bin),
      data.speed_hist.map(d=>d.count));
  }
  if (data.speed_buckets && data.speed_buckets.length) {
    makeBar('chart-speed-buckets',
      data.speed_buckets.map(d=>d.label),
      data.speed_buckets.map(d=>d.value));
  }
  if (data.altitude_hist && data.altitude_hist.length) {
    makeBar('chart-altitude',
      data.altitude_hist.map(d=>d.bin),
      data.altitude_hist.map(d=>d.count));
  }
  if (data.compass && data.compass.length) renderCompass(data.compass);
}

// ── compass ──
function renderCompass(data) {
  const svg = document.getElementById('compass-svg');
  const cx=100, cy=100, r=80;
  const maxv = Math.max(...data.map(d=>d.count));
  let paths = '';
  const n = data.length;
  data.forEach((d,i) => {
    const a0 = (i*360/n - 90) * Math.PI/180;
    const a1 = ((i+1)*360/n - 90) * Math.PI/180;
    const len = maxv>0 ? (d.count/maxv)*r : 0;
    const x1=cx+Math.cos(a0)*len, y1=cy+Math.sin(a0)*len;
    const x2=cx+Math.cos(a1)*len, y2=cy+Math.sin(a1)*len;
    const t = maxv>0 ? d.count/maxv : 0;
    const rr=Math.round(t*0+(1-t)*0), gg=Math.round(t*255+(1-t)*100), bb=Math.round(t*224+(1-t)*255);
    paths += `<path d="M${cx},${cy} L${x1.toFixed(1)},${y1.toFixed(1)} A${len.toFixed(1)},${len.toFixed(1)} 0 0,1 ${x2.toFixed(1)},${y2.toFixed(1)} Z" fill="rgba(${rr},${gg},${bb},0.8)"/>`;
  });
  // rings
  let rings = '';
  [0.25,0.5,0.75,1].forEach(s => {
    rings += `<circle cx="${cx}" cy="${cy}" r="${r*s}" fill="none" stroke="#1a2830" stroke-width="1"/>`;
  });
  // cardinal labels
  const labels = [{t:'N',a:-90},{t:'E',a:0},{t:'S',a:90},{t:'W',a:180}];
  let ltxt = labels.map(l => {
    const a=l.a*Math.PI/180;
    const x=cx+Math.cos(a)*(r+12), y=cy+Math.sin(a)*(r+12);
    return `<text x="${x.toFixed(0)}" y="${y.toFixed(0)}" text-anchor="middle" dominant-baseline="middle" fill="#00ffe0" font-size="11" font-family="'Share Tech Mono'">${l.t}</text>`;
  }).join('');
  svg.innerHTML = rings + paths + ltxt;
}

// ── network ──
function renderNetwork(data) {
  const container = document.getElementById('network-charts');
  const keys = ['wifi_ssid','connected_wifi_vendor_name','carrier'];
  let html = '';
  keys.forEach(k => {
    if (data[k] && data[k].length)
      html += `<div class="card"><div class="card-title">⊞ ${k.replace(/_/g,' ').toUpperCase()}</div><canvas id="nchart-${k}"></canvas></div>`;
  });
  if (data.ip_enrichment_sample) {
    html += `<div class="card"><div class="card-title">⊞ IP ENRICHMENT SAMPLES</div><div id="ip-samples" style="font-family:var(--font-mono);font-size:11px;color:var(--muted)"></div></div>`;
  }
  container.innerHTML = html;
  keys.forEach(k => {
    if (data[k] && data[k].length)
      makeBar(`nchart-${k}`, data[k].map(d=>d.label), data[k].map(d=>d.value), {horizontal:true});
  });
  if (data.ip_enrichment_sample) {
    document.getElementById('ip-samples').innerHTML = data.ip_enrichment_sample.map(s=>`<div style="margin:6px 0;padding:6px;border:1px solid #1a2830">${s}</div>`).join('');
  }
}

// ── anomalies ──
function renderAnomalies(data) {
  const tbody = document.getElementById('anomaly-tbody');
  const colors = {critical:GLOW, high:WARN, medium:'#ffd23f', low:'#39ff8a'};
  tbody.innerHTML = data.map(a => `
    <tr>
      <td ${a.details ? 'style="border-bottom:none"' : ''}><span class="badge ${a.severity}">${a.severity}</span></td>
      <td ${a.details ? 'style="border-bottom:none"' : ''}>${a.type}</td>
      <td style="color:${colors[a.severity]||GLOW};text-shadow:0 0 8px ${colors[a.severity]||GLOW}; ${a.details ? 'border-bottom:none' : ''}">${a.count.toLocaleString()}</td>
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
  if (!data.length) {
    mini.innerHTML = '<p style="color:var(--ok);font-family:var(--font-mono);padding:20px;text-align:center">✓ CLEAN</p>';
    return;
  }
  mini.innerHTML = data.map(a => `
    <div style="display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-bottom:1px solid var(--border)">
      <span style="font-family:var(--font-mono);font-size:11px">${a.type}</span>
      <div style="text-align:right">
        <span class="badge ${a.severity}" style="margin-right:8px">${a.severity}</span>
        <span style="font-family:var(--font-mono);color:${colors[a.severity]||GLOW}">${a.count.toLocaleString()}</span>
      </div>
    </div>`).join('');
}

// ── associations ──
function renderAssociations(data) {
  if (!data.length) return;
  const container = document.getElementById('assoc-container');
  container.innerHTML = `<table class="assoc-table">
    <thead><tr><th>Entity</th><th>Unit</th><th>Avg Score</th><th>Obs</th></tr></thead>
    <tbody>${data.map(r=>`
      <tr>
        <td style="color:var(--glow2)">${r.entity_id||'—'}</td>
        <td>${r.unit_name||'—'}</td>
        <td>
          ${(r.avg_score||0).toFixed(3)}
          <div class="score-bar"><div class="score-fill" style="width:${((r.avg_score||0)*100).toFixed(1)}%"></div></div>
        </td>
        <td style="color:var(--muted)">${r.observations||0}</td>
      </tr>`).join('')}
    </tbody>
  </table>`;
}

// ── bootstrap ──
async function boot() {
  try {
    const res  = await fetch('/data');
    D = await res.json();
    document.getElementById('loading').style.display='none';
    document.getElementById('data-meta').textContent =
      `${D.summary.total_events.toLocaleString()} EVENTS · ${D.summary.unique_entities.toLocaleString()} ENTITIES · ${D.summary.time_span_days}d WINDOW`;

    renderKPIs(D.summary);
    renderHeatmap(D.heatmap);
    renderTimeline(D.timeline);
    // renderGeo(D.geo); // Delegated to showTab('geo')
    renderUnits(D.units);
    renderDevices(D.devices);
    renderMovement(D.movement);
    renderNetwork(D.network);
    renderAnomalies(D.anomalies);
    renderAssociations(D.associations);
  } catch(e) {
    document.getElementById('loading').querySelector('p').textContent = 'ERROR: '+e.message;
  }
}
boot();
</script>
</body>
</html>"""


# ── Entry point ───────────────────────────────────────────────────────────────


def load_from_clickhouse(host: str) -> dict:
    import time

    print(f"[+] Connecting to ClickHouse at {host}:8123...")

    client = None
    for attempt in range(1, 21):
        try:
            client = clickhouse_connect.get_client(host=host, port=8123)
            # Check if table exists
            tables = client.query("SHOW TABLES").result_rows
            break
        except Exception as e:
            print(
                f"[-] ClickHouse or network not ready yet (attempt {attempt}/20): {e}",
                flush=True,
            )
            time.sleep(5)

    if not client:
        print(
            "[-] Could not connect to ClickHouse after 20 attempts. Using empty DataFrame."
        )
        df = pd.DataFrame()
    else:
        try:
            if not any(t[0] == "sigint_data" for t in tables):
                print(
                    "[-] ClickHouse table 'sigint_data' does not exist yet. Using empty DataFrame."
                )
                df = pd.DataFrame()
            else:
                print("[+] Querying sigint_data table...")
                df = client.query_df("SELECT * FROM sigint_data")
                print(f"[+] Pulled {len(df):,} rows from ClickHouse")
        except Exception as e:
            print(f"[-] Error querying ClickHouse: {e}")
            df = pd.DataFrame()

    # Parse timestamps
    for col in ["event_time", "first_seen", "last_seen"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    # Numeric coercions
    for col in [
        "latitude",
        "longitude",
        "speed",
        "heading",
        "altitude",
        "horizontal_accuracy",
        "vertical_accuracy",
        "event_location_accuracy_score",
        "device_to_unit_association_score",
        "device_at_unit_site_occupancy_metric",
        "entity_age",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return {
        "summary": fuse_summary(df),
        "timeline": fuse_timeline(df),
        "geo": fuse_geo(df),
        "units": fuse_units(df),
        "devices": fuse_devices(df),
        "movement": fuse_movement(df),
        "heatmap": fuse_heatmap(df),
        "associations": fuse_associations(df),
        "anomalies": fuse_anomalies(df),
        "network": fuse_network(df),
    }


def main():
    host = os.environ.get("CLICKHOUSE_HOST")
    if host:
        print("[+] ClickHouse backend detected. Pulling live data...")
        fused = load_from_clickhouse(host)
    else:
        if len(sys.argv) < 2:
            print(
                "Usage: python analyze.py <path_to_csv_or_dir>   (OR set CLICKHOUSE_HOST)"
            )
            sys.exit(1)

        csv_path = sys.argv[1]
        if not Path(csv_path).exists():
            print(f"Error: path not found — {csv_path}")
            sys.exit(1)

        print("[+] Fusing data from local CSV...")
        fused = load_and_fuse(csv_path)

    DATA_CACHE.update(fused)
    print(f"[+] Summary: {json.dumps(fused['summary'], indent=2)}")

    port = 8000
    server = HTTPServer(("0.0.0.0", port), Handler)
    print(f"\n[+] Dashboard API and Client running on http://0.0.0.0:{port}")
    print("[+] Press Ctrl+C to stop\n")

    if not host:
        threading.Timer(
            1.0, lambda: webbrowser.open(f"http://localhost:{port}")
        ).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[+] Server stopped.")


if __name__ == "__main__":
    main()
