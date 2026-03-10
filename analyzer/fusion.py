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
