import math
import json
from pathlib import Path
import pandas as pd
import numpy as np


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
        mean_freq = freq.mean()
        std_freq = freq.std()
        threshold = (
            mean_freq + (3 * std_freq) if not math.isnan(std_freq) else mean_freq
        )
        burst = freq[freq > threshold]
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
                    "desc": f"Entities with unusually high event counts (>{int(threshold)})",
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
    return out


# ---------------------------------------------------------------------------
# ── Behavioral Analytics ──────────────────────────────────────────────────
# ---------------------------------------------------------------------------


def _haversine_km(lat1, lon1, lat2, lon2):
    """Vectorised haversine distance in km."""
    R = 6371.0
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return R * 2 * np.arcsin(np.sqrt(a))


def fuse_colocation(df):
    """Detect entity pairs co-located within ~100 m / 30 min."""
    needed = {"entity_id", "latitude", "longitude", "event_time"}
    if not needed.issubset(df.columns):
        return []
    sub = df[list(needed)].dropna().sort_values("event_time")
    if len(sub) > 20000:
        sub = sub.sample(20000, random_state=42)
    if len(sub) < 2:
        return []

    # Round lat/lon to ~100 m grid cells and time to 30 min buckets
    sub = sub.copy()
    sub["grid_lat"] = (sub["latitude"] * 100).round().astype(int)
    sub["grid_lon"] = (sub["longitude"] * 100).round().astype(int)
    sub["time_bucket"] = sub["event_time"].dt.floor("30min")

    grouped = sub.groupby(["grid_lat", "grid_lon", "time_bucket"])["entity_id"].apply(
        set
    )
    pairs = {}
    for entities in grouped:
        if len(entities) < 2 or len(entities) > 20:
            continue
        elist = sorted(entities)
        for i in range(len(elist)):
            for j in range(i + 1, len(elist)):
                key = (elist[i], elist[j])
                pairs[key] = pairs.get(key, 0) + 1

    top = sorted(pairs.items(), key=lambda x: -x[1])[:50]
    return [{"entity_a": a, "entity_b": b, "co_events": c} for (a, b), c in top]


def fuse_cotravel(df):
    """Entity pairs co-located at ≥3 distinct locations."""
    needed = {"entity_id", "latitude", "longitude"}
    if not needed.issubset(df.columns):
        return []
    sub = df[list(needed)].dropna()
    if len(sub) > 20000:
        sub = sub.sample(20000, random_state=42)
    if len(sub) < 2:
        return []

    sub = sub.copy()
    sub["grid_lat"] = (sub["latitude"] * 100).round().astype(int)
    sub["grid_lon"] = (sub["longitude"] * 100).round().astype(int)
    sub["grid"] = sub["grid_lat"].astype(str) + "," + sub["grid_lon"].astype(str)

    grouped = sub.groupby("grid")["entity_id"].apply(set)
    pair_locations = {}
    for grid_cell, entities in grouped.items():
        if len(entities) < 2 or len(entities) > 20:
            continue
        elist = sorted(entities)
        for i in range(len(elist)):
            for j in range(i + 1, len(elist)):
                key = (elist[i], elist[j])
                if key not in pair_locations:
                    pair_locations[key] = set()
                pair_locations[key].add(grid_cell)

    cotravel = {k: len(v) for k, v in pair_locations.items() if len(v) >= 3}
    top = sorted(cotravel.items(), key=lambda x: -x[1])[:50]
    return [{"entity_a": a, "entity_b": b, "shared_locations": c} for (a, b), c in top]


def fuse_conetwork(df):
    """Entities sharing same WiFi SSID or carrier at the same site."""
    out = []
    for net_col in ["wifi_ssid", "carrier"]:
        if net_col not in df.columns or "entity_id" not in df.columns:
            continue
        sub = df[["entity_id", net_col]].dropna()
        if sub.empty:
            continue
        grouped = sub.groupby(net_col)["entity_id"].apply(set)
        for net_name, entities in grouped.items():
            if len(entities) < 2 or len(entities) > 30:
                continue
            out.append(
                {
                    "network_type": net_col,
                    "network_name": str(net_name),
                    "entity_count": len(entities),
                    "entities": sorted(entities)[:10],
                }
            )
    out.sort(key=lambda x: -x["entity_count"])
    return out[:40]


def fuse_geofence(df):
    """Geofence entry/exit detection using configured zones.
    Zones loaded from GEOFENCE_CONFIG env var (JSON file)."""
    import os, json as _json

    needed = {"entity_id", "latitude", "longitude", "event_time"}
    if not needed.issubset(df.columns):
        return {"zones": [], "events": []}

    config_path = os.environ.get("GEOFENCE_CONFIG", "")
    zones = []
    if config_path and os.path.exists(config_path):
        try:
            with open(config_path) as f:
                zones = _json.load(f)
        except Exception:
            pass

    # Default demo zones if none configured
    if not zones:
        zones = [
            {"name": "ZONE-ALPHA", "lat": 32.0, "lon": 53.0, "radius_km": 50},
            {"name": "ZONE-BRAVO", "lat": 35.7, "lon": 51.4, "radius_km": 30},
            {"name": "ZONE-CHARLIE", "lat": 29.6, "lon": 52.5, "radius_km": 40},
        ]

    sub = df[list(needed)].dropna().sort_values("event_time")
    if len(sub) > 30000:
        sub = sub.sample(30000, random_state=42)

    events = []
    for zone in zones:
        zlat, zlon, zr = zone["lat"], zone["lon"], zone["radius_km"]
        dists = _haversine_km(
            sub["latitude"].values, sub["longitude"].values, zlat, zlon
        )
        inside = dists <= zr
        zone_sub = sub[inside]
        entity_counts = zone_sub["entity_id"].nunique() if not zone_sub.empty else 0
        event_counts = len(zone_sub)
        zone["entities_detected"] = int(entity_counts)
        zone["total_events"] = int(event_counts)

        if not zone_sub.empty:
            # Top entities in zone
            top_entities = zone_sub["entity_id"].value_counts().head(5)
            for eid, cnt in top_entities.items():
                entity_times = zone_sub[zone_sub["entity_id"] == eid]["event_time"]
                dwell_mins = 0
                if len(entity_times) > 1:
                    dwell_mins = int(
                        (entity_times.max() - entity_times.min()).total_seconds() / 60
                    )
                events.append(
                    {
                        "zone": zone["name"],
                        "entity_id": str(eid),
                        "observations": int(cnt),
                        "dwell_minutes": dwell_mins,
                    }
                )

    return {"zones": zones, "events": events[:60]}


def fuse_dwell(df):
    """Group consecutive events per entity within ~200 m into dwell sessions."""
    needed = {"entity_id", "latitude", "longitude", "event_time"}
    if not needed.issubset(df.columns):
        return []
    sub = df[list(needed)].dropna().sort_values(["entity_id", "event_time"])
    if len(sub) > 30000:
        sub = sub.sample(30000, random_state=42).sort_values(
            ["entity_id", "event_time"]
        )

    sessions = []
    for eid, grp in sub.groupby("entity_id"):
        if len(grp) < 2:
            continue
        lats = grp["latitude"].values
        lons = grp["longitude"].values
        times = grp["event_time"].values

        session_start = 0
        for i in range(1, len(grp)):
            dist = _haversine_km(
                lats[i], lons[i], lats[session_start], lons[session_start]
            )
            if dist > 0.2:  # > 200m = new session
                duration = (times[i - 1] - times[session_start]) / np.timedelta64(
                    1, "m"
                )
                if duration > 5:  # at least 5 min dwell
                    sessions.append(
                        {
                            "entity_id": str(eid),
                            "lat": round(float(np.mean(lats[session_start:i])), 5),
                            "lon": round(float(np.mean(lons[session_start:i])), 5),
                            "dwell_minutes": round(float(duration), 1),
                            "observations": i - session_start,
                        }
                    )
                session_start = i

    sessions.sort(key=lambda x: -x["dwell_minutes"])
    return sessions[:60]


# ---------------------------------------------------------------------------
# ── Multi-Domain Intelligence ─────────────────────────────────────────────
# ---------------------------------------------------------------------------


def _domain_value_counts(df, columns, limit=12):
    """Generic domain breakdown for columns that may or may not exist."""
    out = {}
    for col in columns:
        if col in df.columns and df[col].notna().any():
            vc = df[col].value_counts().head(limit)
            out[col] = [{"label": str(k), "value": int(v)} for k, v in vc.items()]
    return out


def fuse_maritime(df):
    """AIS / SHADOWFLEET™ maritime domain analysis."""
    cols = [
        "mmsi",
        "imo",
        "vessel_name",
        "flag_state",
        "ship_type",
        "vessel_status",
        "destination",
        "cargo_type",
    ]
    out = _domain_value_counts(df, cols)
    out["_has_data"] = bool(out)
    if "mmsi" in df.columns:
        out["unique_vessels"] = int(df["mmsi"].nunique())
    return out


def fuse_aviation(df):
    """ADS-B aviation domain analysis."""
    cols = [
        "icao24",
        "callsign",
        "flight_number",
        "squawk",
        "aircraft_type",
        "airline",
        "origin_airport",
        "dest_airport",
    ]
    out = _domain_value_counts(df, cols)
    out["_has_data"] = bool({k: v for k, v in out.items() if k != "_has_data"})
    if "icao24" in df.columns:
        out["unique_aircraft"] = int(df["icao24"].nunique())
    return out


def fuse_cyber(df):
    """Cyber domain — IP, domain, threat indicators."""
    cols = [
        "ip_address",
        "domain",
        "user_agent",
        "threat_category",
        "malware_family",
        "protocol",
        "port",
    ]
    out = _domain_value_counts(df, cols)
    out["_has_data"] = bool({k: v for k, v in out.items() if k != "_has_data"})
    if "threat_score" in df.columns:
        s = pd.to_numeric(df["threat_score"], errors="coerce").dropna()
        if not s.empty:
            hist, edges = np.histogram(s, bins=20)
            out["threat_score_hist"] = [
                {"bin": round(float(edges[i]), 2), "count": int(hist[i])}
                for i in range(len(hist))
            ]
    return out


def fuse_rf(df):
    """RF / SIGINT domain — frequency, emitter analysis."""
    cols = [
        "frequency_mhz",
        "modulation",
        "emitter_id",
        "signal_type",
        "bandwidth_khz",
        "polarization",
    ]
    out = _domain_value_counts(df, cols)
    out["_has_data"] = bool({k: v for k, v in out.items() if k != "_has_data"})
    if "signal_strength" in df.columns:
        s = pd.to_numeric(df["signal_strength"], errors="coerce").dropna()
        if not s.empty:
            hist, edges = np.histogram(s, bins=20)
            out["signal_strength_hist"] = [
                {"bin": round(float(edges[i]), 2), "count": int(hist[i])}
                for i in range(len(hist))
            ]
    return out


def fuse_osint(df):
    """OSINT domain — source, platform, sentiment."""
    cols = [
        "source_url",
        "source_platform",
        "language",
        "author",
        "content_type",
        "media_type",
    ]
    out = _domain_value_counts(df, cols)
    out["_has_data"] = bool({k: v for k, v in out.items() if k != "_has_data"})
    if "sentiment_score" in df.columns:
        s = pd.to_numeric(df["sentiment_score"], errors="coerce").dropna()
        if not s.empty:
            bins = [-1, -0.5, -0.1, 0.1, 0.5, 1.0]
            labels = [
                "Very Negative",
                "Negative",
                "Neutral",
                "Positive",
                "Very Positive",
            ]
            cats = pd.cut(s, bins=bins, labels=labels)
            vc = cats.value_counts().reindex(labels).fillna(0)
            out["sentiment_breakdown"] = [
                {"label": l, "value": int(v)} for l, v in zip(vc.index, vc.values)
            ]
    return out


# ---------------------------------------------------------------------------
# ── SKYTRACE™ — Satellite ISP Correlation ─────────────────────────────────
# ---------------------------------------------------------------------------


def fuse_skytrace(df):
    """Satellite ISP usage correlation."""
    cols = [
        "isp_name",
        "connection_type",
        "satellite_provider",
        "ip_geo_country",
        "ip_geo_city",
        "vpn_detected",
    ]
    out = _domain_value_counts(df, cols)
    out["_has_data"] = bool({k: v for k, v in out.items() if k != "_has_data"})

    # Entity ↔ ISP correlation
    if "entity_id" in df.columns and "isp_name" in df.columns:
        sub = df[["entity_id", "isp_name"]].dropna()
        if not sub.empty:
            ct = sub.groupby(["entity_id", "isp_name"]).size().reset_index(name="count")
            ct = ct.sort_values("count", ascending=False).head(30)
            out["entity_isp_corr"] = df_to_records(ct)

    return out


# ---------------------------------------------------------------------------
# ── Geocoding & Translation ───────────────────────────────────────────────
# ---------------------------------------------------------------------------


def geocode_summary(df):
    """Approximate reverse-geocode using lat/lon region buckets."""
    if "latitude" not in df.columns or "longitude" not in df.columns:
        return []
    sub = df[["latitude", "longitude"]].dropna()
    if sub.empty:
        return []
    # Approximate region labels by lat/lon ranges
    regions = [
        {
            "name": "Middle East",
            "lat_min": 12,
            "lat_max": 42,
            "lon_min": 25,
            "lon_max": 65,
        },
        {"name": "Europe", "lat_min": 35, "lat_max": 72, "lon_min": -10, "lon_max": 40},
        {
            "name": "East Asia",
            "lat_min": 15,
            "lat_max": 55,
            "lon_min": 100,
            "lon_max": 150,
        },
        {
            "name": "South Asia",
            "lat_min": 5,
            "lat_max": 40,
            "lon_min": 60,
            "lon_max": 100,
        },
        {
            "name": "Africa",
            "lat_min": -35,
            "lat_max": 37,
            "lon_min": -20,
            "lon_max": 55,
        },
        {
            "name": "North America",
            "lat_min": 15,
            "lat_max": 72,
            "lon_min": -170,
            "lon_max": -50,
        },
        {
            "name": "South America",
            "lat_min": -56,
            "lat_max": 15,
            "lon_min": -82,
            "lon_max": -34,
        },
        {
            "name": "Oceania",
            "lat_min": -50,
            "lat_max": 0,
            "lon_min": 110,
            "lon_max": 180,
        },
    ]
    results = []
    for r in regions:
        mask = (
            (sub["latitude"] >= r["lat_min"])
            & (sub["latitude"] <= r["lat_max"])
            & (sub["longitude"] >= r["lon_min"])
            & (sub["longitude"] <= r["lon_max"])
        )
        cnt = int(mask.sum())
        if cnt > 0:
            results.append({"region": r["name"], "events": cnt})
    results.sort(key=lambda x: -x["events"])
    return results


def fuse_translation(df):
    """Stub for language detection, translation, and image analysis metadata."""
    out = {"_has_data": False}
    # Language detection on text fields
    text_cols = [
        c
        for c in df.columns
        if any(k in c.lower() for k in ["text", "message", "content", "description"])
    ]
    if text_cols:
        out["text_columns_detected"] = text_cols
        out["_has_data"] = True
        # Simple language heuristic: check for non-ASCII chars ratio
        for col in text_cols[:3]:
            vals = df[col].dropna().astype(str)
            if vals.empty:
                continue
            sample = vals.head(100)
            ascii_ratio = sample.apply(
                lambda x: sum(1 for c in x if ord(c) < 128) / max(len(x), 1)
            ).mean()
            out[f"{col}_ascii_ratio"] = round(float(ascii_ratio), 3)
            out[f"{col}_sample_count"] = int(len(vals))

    # Image/media columns
    media_cols = [
        c
        for c in df.columns
        if any(k in c.lower() for k in ["image", "photo", "media", "attachment"])
    ]
    if media_cols:
        out["media_columns_detected"] = media_cols
        out["_has_data"] = True

    return out


# ---------------------------------------------------------------------------
# ── Updated load_and_fuse ─────────────────────────────────────────────────
# ---------------------------------------------------------------------------


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
        "colocation": fuse_colocation(df),
        "cotravel": fuse_cotravel(df),
        "conetwork": fuse_conetwork(df),
        "geofence": fuse_geofence(df),
        "dwell": fuse_dwell(df),
        "maritime": fuse_maritime(df),
        "aviation": fuse_aviation(df),
        "cyber": fuse_cyber(df),
        "rf": fuse_rf(df),
        "osint": fuse_osint(df),
        "skytrace": fuse_skytrace(df),
        "geocode": geocode_summary(df),
        "translation": fuse_translation(df),
    }
