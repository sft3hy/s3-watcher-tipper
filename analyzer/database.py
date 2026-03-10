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
                # Optimized query: select only needed columns and limit to 500k rows
                query = """
                    SELECT 
                        event_time, first_seen, last_seen,
                        latitude, longitude, speed, heading, altitude,
                        horizontal_accuracy, vertical_accuracy,
                        event_location_accuracy_score,
                        device_to_unit_association_score,
                        device_at_unit_site_occupancy_metric,
                        entity_age, entity_id, unit_id, site_id, country_code_1,
                        unit_name, unit_echelon, unit_domain, 
                        unit_type_level_1, unit_type_level_2,
                        regional_command, operational_command, orbat,
                        device_brand, platform, carrier, app_id,
                        wifi_ssid, connected_wifi_vendor_name, ip_enrichment,
                        meta_row_id
                    FROM sigint_data
                    ORDER BY event_time DESC
                    LIMIT 500000
                """
                df = client.query_df(query)
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
