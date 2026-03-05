import boto3
import time
import os
import tempfile
import json
import pandas as pd
from datetime import datetime, timedelta
from cs_helpers import send_public_message

# ── Configuration (from environment variables) ────────────────────────────────
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
PREFIX = os.environ.get("S3_PREFIX", "")
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "3600"))
# ─────────────────────────────────────────────────────────────────────────────

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)


def list_objects(bucket, prefix=""):
    objects = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects[obj["Key"]] = obj["ETag"]
    return objects


def process_parquet_file(bucket, key):
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
            tmp_path = tmp.name

        s3.download_file(bucket, key, tmp_path)

        df = pd.read_parquet(tmp_path)
        records = df.to_dict("records")

        count = 0
        for row in records:
            message_text = json.dumps(row)
            send_public_message(roomName="atreides_data", message=message_text)
            count += 1

        os.remove(tmp_path)
        print(
            f"[{now}] Successfully sent {count} records to ChatSurfer from {key}",
            flush=True,
        )
    except Exception as e:
        print(f"[{now}] Error processing parquet file {key}: {e}", flush=True)


def read_object(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8", errors="replace")


def check_for_changes(bucket, prefix, previous_state):
    current_state = list_objects(bucket, prefix)
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    new_keys = set(current_state) - set(previous_state)
    deleted_keys = set(previous_state) - set(current_state)
    modified_keys = {
        k
        for k in current_state
        if k in previous_state and current_state[k] != previous_state[k]
    }

    if not (new_keys or deleted_keys or modified_keys):
        print(f"[{now}] No changes detected.", flush=True)
        return current_state

    for key in sorted(new_keys):
        print(f"\n{'='*60}", flush=True)
        print(f"[{now}] NEW FILE: s3://{bucket}/{key}", flush=True)
        print(f"{'='*60}", flush=True)
        if key.endswith(".parquet"):
            process_parquet_file(bucket, key)
        else:
            try:
                content = read_object(bucket, key)
                print(content, flush=True)
                message_text = f"New file: {key}\n\n{content}"
                send_public_message(roomName="atreides_data", message=message_text)
            except Exception as e:
                print(f"  [error reading file: {e}]", flush=True)

    for key in sorted(modified_keys):
        print(f"\n{'='*60}", flush=True)
        print(f"[{now}] MODIFIED FILE: s3://{bucket}/{key}", flush=True)
        print(f"{'='*60}", flush=True)
        if key.endswith(".parquet"):
            process_parquet_file(bucket, key)
        else:
            try:
                print(read_object(bucket, key), flush=True)
            except Exception as e:
                print(f"  [error reading file: {e}]", flush=True)

    for key in sorted(deleted_keys):
        print(f"\n[{now}] DELETED FILE: s3://{bucket}/{key}", flush=True)

    return current_state


if __name__ == "__main__":
    print(
        f"Starting S3 watcher. Base prefix: s3://{BUCKET_NAME}/{PREFIX} — polling every {POLL_INTERVAL_SECONDS}s",
        flush=True,
    )

    # We use a state dictionary keyed by the dynamic prefix so we don't cross signals
    states = {}

    while True:
        target_date = (datetime.utcnow() - timedelta(days=3)).strftime("%Y-%m-%d")

        # Build dynamic prefix. Only append the slash if PREFIX is non-empty and missing it.
        if PREFIX and not PREFIX.endswith("/"):
            dynamic_prefix = f"{PREFIX}/date={target_date}/"
        elif PREFIX:
            dynamic_prefix = f"{PREFIX}date={target_date}/"
        else:
            dynamic_prefix = f"date={target_date}/"

        if dynamic_prefix not in states:
            states[dynamic_prefix] = list_objects(BUCKET_NAME, dynamic_prefix)
            print(
                f"[{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}] Initializing state for {dynamic_prefix}. {len(states[dynamic_prefix])} object(s) currently found.",
                flush=True,
            )
        else:
            states[dynamic_prefix] = check_for_changes(
                BUCKET_NAME, dynamic_prefix, states[dynamic_prefix]
            )

        time.sleep(POLL_INTERVAL_SECONDS)
