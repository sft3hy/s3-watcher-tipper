import os
import pandas as pd
import clickhouse_connect
from .fusion import (
    fuse_summary,
    fuse_timeline,
    fuse_geo,
    fuse_units,
    fuse_devices,
    fuse_movement,
    fuse_heatmap,
    fuse_associations,
    fuse_anomalies,
    fuse_network,
)

# ---------------------------------------------------------------------------
# ClickHouse has only ~3.6 GiB RAM.  A single wide SELECT blows it up.
# Strategy: run many **small** queries, each fetching only the columns the
# corresponding fusion function needs.  Where possible we push aggregation
# into ClickHouse so the transfer is tiny.
# ---------------------------------------------------------------------------

# Shared query settings — keep individual query memory low
_QS = {"max_memory_usage": 1_500_000_000}  # 1.5 GiB per query
_TIME_FILTER = "WHERE event_time >= now() - INTERVAL 90 DAY"
_ROW_CAP = 50000


def _q(client, sql, label="query"):
    """Run a query and return a DataFrame, or empty on failure."""
    try:
        df = client.query_df(sql, settings=_QS)
        print(f"    ✓ {label}: {len(df):,} rows")
        return df
    except Exception as e:
        print(f"    ✗ {label}: {e}")
        return pd.DataFrame()


def _coerce_timestamps(df, cols=("event_time", "first_seen", "last_seen")):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)


def _coerce_numerics(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")


def load_from_clickhouse(host: str) -> dict:
    import time

    user = os.environ.get("CLICKHOUSE_USER", "default")
    password = os.environ.get("CLICKHOUSE_PASSWORD", "")
    print(f"[+] Connecting to ClickHouse at {host}:8123 as {user}...")

    client = None
    for attempt in range(1, 21):
        try:
            client = clickhouse_connect.get_client(
                host=host, port=8123, username=user, password=password
            )
            tables = client.query("SHOW TABLES").result_rows
            break
        except Exception as e:
            print(
                f"[-] ClickHouse or network not ready yet (attempt {attempt}/20): {e}",
                flush=True,
            )
            time.sleep(5)

    if not client:
        print("[-] Could not connect to ClickHouse after 20 attempts.")
        return _empty_result()

    if not any(t[0] == "sigint_data" for t in tables):
        print("[-] ClickHouse table 'sigint_data' does not exist yet.")
        return _empty_result()

    print("[+] Querying sigint_data table (multi-query mode)...")

    # ── 1. Summary (server-side aggregation — tiny result) ────────────
    df_summary_raw = _q(
        client,
        f"""
        SELECT
            count()                             AS total_events,
            uniqExact(entity_id)                AS unique_entities,
            uniqExact(unit_id)                  AS unique_units,
            uniqExact(site_id)                  AS unique_sites,
            uniqExact(country_code_1)           AS countries,
            countIf(latitude IS NOT NULL AND latitude != 0) AS has_geo,
            min(event_time)                     AS min_time,
            max(event_time)                     AS max_time
        FROM sigint_data
        {_TIME_FILTER}
    """,
        "summary",
    )

    if not df_summary_raw.empty:
        r = df_summary_raw.iloc[0]
        min_t = pd.to_datetime(r["min_time"], errors="coerce", utc=True)
        max_t = pd.to_datetime(r["max_time"], errors="coerce", utc=True)
        span = (max_t - min_t).days if pd.notna(min_t) and pd.notna(max_t) else 0
        summary = {
            "total_events": int(r["total_events"]),
            "unique_entities": int(r["unique_entities"]),
            "unique_units": int(r["unique_units"]),
            "unique_sites": int(r["unique_sites"]),
            "time_span_days": int(span),
            "countries": int(r["countries"]),
            "has_geo": int(r["has_geo"]),
            "null_pct": 0,  # expensive to compute exactly; skip
        }
    else:
        summary = fuse_summary(pd.DataFrame())

    # ── 2. Timeline (server-side GROUP BY) ────────────────────────────
    df_timeline = _q(
        client,
        f"""
        SELECT toDate(event_time) AS date, count() AS count
        FROM sigint_data
        {_TIME_FILTER}
        GROUP BY date ORDER BY date
    """,
        "timeline",
    )

    if not df_timeline.empty:
        import numpy as np

        vals = df_timeline["count"].values.astype(float)
        roll7 = __import__("numpy").convolve(
            vals, __import__("numpy").ones(7) / 7, mode="same"
        )
        timeline = [
            {"date": str(d), "count": int(c), "rolling7": round(float(r), 1)}
            for d, c, r in zip(df_timeline["date"], vals, roll7)
        ]
    else:
        timeline = []

    # ── 3. Geo (only lat/lon + a few labels, sampled) ─────────────────
    df_geo = _q(
        client,
        f"""
        SELECT latitude, longitude, entity_id, unit_name,
               speed, event_location_accuracy_score
        FROM sigint_data
        {_TIME_FILTER}
          AND latitude IS NOT NULL AND longitude IS NOT NULL
          AND latitude != 0 AND longitude != 0
        ORDER BY rand()
        LIMIT 5000
    """,
        "geo",
    )
    _coerce_numerics(
        df_geo, ["latitude", "longitude", "speed", "event_location_accuracy_score"]
    )
    geo = fuse_geo(df_geo)

    # ── 4. Units (value counts — push into CH) ───────────────────────
    unit_cols = [
        "unit_echelon",
        "unit_domain",
        "unit_type_level_1",
        "unit_type_level_2",
        "regional_command",
        "operational_command",
        "orbat",
    ]
    units_out = {}
    for col in unit_cols:
        df_uc = _q(
            client,
            f"""
            SELECT {col} AS label, count() AS value
            FROM sigint_data
            {_TIME_FILTER} AND {col} IS NOT NULL AND {col} != ''
            GROUP BY {col}
            ORDER BY value DESC
            LIMIT 15
        """,
            f"units/{col}",
        )
        if not df_uc.empty:
            units_out[col] = [
                {"label": str(row["label"]), "value": int(row["value"])}
                for _, row in df_uc.iterrows()
            ]

    # Score histograms — pull small sample
    df_scores = _q(
        client,
        f"""
        SELECT device_to_unit_association_score,
               device_at_unit_site_occupancy_metric
        FROM sigint_data
        {_TIME_FILTER}
          AND (device_to_unit_association_score IS NOT NULL
               OR device_at_unit_site_occupancy_metric IS NOT NULL)
        LIMIT {_ROW_CAP}
    """,
        "unit_scores",
    )
    _coerce_numerics(
        df_scores,
        ["device_to_unit_association_score", "device_at_unit_site_occupancy_metric"],
    )
    import numpy as np

    scores = {}
    for col in [
        "device_to_unit_association_score",
        "device_at_unit_site_occupancy_metric",
    ]:
        if col in df_scores.columns:
            s = df_scores[col].dropna()
            if not s.empty:
                hist, edges = np.histogram(s, bins=20)
                scores[col] = [
                    {"bin": round(float(edges[i]), 3), "count": int(hist[i])}
                    for i in range(len(hist))
                ]
    units_out["score_histograms"] = scores

    # ── 5. Devices (value counts — push into CH) ─────────────────────
    devices_out = {}
    for col in ["device_brand", "platform", "carrier", "app_id"]:
        df_dc = _q(
            client,
            f"""
            SELECT {col} AS label, count() AS value
            FROM sigint_data
            {_TIME_FILTER} AND {col} IS NOT NULL AND {col} != ''
            GROUP BY {col}
            ORDER BY value DESC
            LIMIT 12
        """,
            f"devices/{col}",
        )
        if not df_dc.empty:
            devices_out[col] = [
                {"label": str(row["label"]), "value": int(row["value"])}
                for _, row in df_dc.iterrows()
            ]

    # Platform × brand matrix
    df_pb = _q(
        client,
        f"""
        SELECT platform, device_brand, count() AS cnt
        FROM sigint_data
        {_TIME_FILTER}
          AND platform IS NOT NULL AND platform != ''
          AND device_brand IS NOT NULL AND device_brand != ''
        GROUP BY platform, device_brand
        ORDER BY cnt DESC
        LIMIT 100
    """,
        "devices/platform_brand",
    )
    if not df_pb.empty:
        ct = df_pb.pivot_table(
            index="platform", columns="device_brand", values="cnt", fill_value=0
        )
        ct = ct.head(8)
        devices_out["platform_brand_matrix"] = {
            "platforms": list(ct.index.astype(str)),
            "brands": list(ct.columns.astype(str)),
            "values": ct.values.tolist(),
        }

    # ── 6. Movement (speed/heading/altitude — small sample) ──────────
    df_move = _q(
        client,
        f"""
        SELECT speed, heading, altitude
        FROM sigint_data
        {_TIME_FILTER}
          AND (speed IS NOT NULL OR heading IS NOT NULL OR altitude IS NOT NULL)
        LIMIT {_ROW_CAP}
    """,
        "movement",
    )
    _coerce_numerics(df_move, ["speed", "heading", "altitude"])
    movement = fuse_movement(df_move)

    # ── 7. Heatmap (server-side aggregation) ──────────────────────────
    df_heat = _q(
        client,
        f"""
        SELECT toDayOfWeek(event_time) AS dow,
               toHour(event_time)      AS hour,
               count()                 AS count
        FROM sigint_data
        {_TIME_FILTER}
        GROUP BY dow, hour
    """,
        "heatmap",
    )
    if not df_heat.empty:
        days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        grid = {}
        for _, row in df_heat.iterrows():
            # ClickHouse toDayOfWeek: 1=Mon … 7=Sun
            grid[(int(row["dow"]) - 1, int(row["hour"]))] = int(row["count"])
        heatmap = [
            {"day": days[d], "hour": h, "count": grid.get((d, h), 0)}
            for d in range(7)
            for h in range(24)
        ]
    else:
        heatmap = []

    # ── 8. Associations (small aggregate) ─────────────────────────────
    df_assoc = _q(
        client,
        f"""
        SELECT entity_id, unit_name,
               avg(device_to_unit_association_score) AS avg_score,
               count() AS observations
        FROM sigint_data
        {_TIME_FILTER}
          AND entity_id IS NOT NULL AND unit_name IS NOT NULL
          AND device_to_unit_association_score IS NOT NULL
        GROUP BY entity_id, unit_name
        ORDER BY avg_score DESC
        LIMIT 40
    """,
        "associations",
    )
    if not df_assoc.empty:
        from .fusion import df_to_records

        _coerce_numerics(df_assoc, ["avg_score", "observations"])
        associations = df_to_records(df_assoc)
    else:
        associations = []

    # ── 9. Anomalies (targeted small queries) ─────────────────────────
    df_anom = _q(
        client,
        f"""
        SELECT entity_id, unit_name, speed, latitude, longitude,
               event_location_accuracy_score,
               device_to_unit_association_score,
               meta_row_id, event_time
        FROM sigint_data
        {_TIME_FILTER}
        LIMIT {_ROW_CAP}
    """,
        "anomalies",
    )
    _coerce_timestamps(df_anom, ["event_time"])
    _coerce_numerics(
        df_anom,
        [
            "speed",
            "latitude",
            "longitude",
            "event_location_accuracy_score",
            "device_to_unit_association_score",
        ],
    )
    anomalies = fuse_anomalies(df_anom)

    # ── 10. Network (value counts — push into CH) ────────────────────
    network_out = {}
    for col in ["wifi_ssid", "connected_wifi_vendor_name", "carrier"]:
        df_nc = _q(
            client,
            f"""
            SELECT {col} AS label, count() AS value
            FROM sigint_data
            {_TIME_FILTER} AND {col} IS NOT NULL AND {col} != ''
            GROUP BY {col}
            ORDER BY value DESC
            LIMIT 10
        """,
            f"network/{col}",
        )
        if not df_nc.empty:
            network_out[col] = [
                {"label": str(row["label"]), "value": int(row["value"])}
                for _, row in df_nc.iterrows()
            ]

    print("[+] All queries complete.")

    return {
        "summary": summary,
        "timeline": timeline,
        "geo": geo,
        "units": units_out,
        "devices": devices_out,
        "movement": movement,
        "heatmap": heatmap,
        "associations": associations,
        "anomalies": anomalies,
        "network": network_out,
    }


def _empty_result():
    """Return a valid empty result dict."""
    df = pd.DataFrame()
    return {
        "summary": fuse_summary(df),
        "timeline": [],
        "geo": [],
        "units": {},
        "devices": {},
        "movement": {},
        "heatmap": [],
        "associations": [],
        "anomalies": [],
        "network": {},
    }
