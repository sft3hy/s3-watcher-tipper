import boto3
import time
import os
import tempfile
import json
import pandas as pd
import logging
from datetime import datetime, timedelta

from pack_parquet_to_csv_zips import pack

# ── Configuration (from environment variables) ────────────────────────────────
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"].strip()
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"].strip()
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
PREFIX = os.environ.get("S3_PREFIX", "")
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "86400"))
# ─────────────────────────────────────────────────────────────────────────────

# Enable verbose debugging for bottom dependencies
# boto3.set_stream_logger("botocore", level=logging.DEBUG)

print("\n" + "=" * 50, flush=True)
print("=== S3 SETUP DEBUGGING ===", flush=True)
print(f"AWS_REGION: {AWS_REGION}", flush=True)
print(f"BUCKET_NAME: {BUCKET_NAME}", flush=True)
print(f"PREFIX: '{PREFIX}'", flush=True)
print(
    f"AWS_ACCESS_KEY_ID length: {len(AWS_ACCESS_KEY_ID)} (ends with: {AWS_ACCESS_KEY_ID[-4:] if len(AWS_ACCESS_KEY_ID) > 4 else '?'})",
    flush=True,
)
print(f"AWS_SECRET_ACCESS_KEY length: {len(AWS_SECRET_ACCESS_KEY)}", flush=True)

try:
    sts = boto3.client(
        "sts",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    identity = sts.get_caller_identity()
    print(f"STS Caller Identity: {identity['Arn']}", flush=True)
except Exception as e:
    print(
        f"STS validation failed. The keys might be invalid or STS is restricted: {e}",
        flush=True,
    )
print("=" * 50 + "\n", flush=True)

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)


def get_most_recent_day_objects(bucket, prefix=""):
    import re

    objects_by_date = {}
    paginator = s3.get_paginator("list_objects_v2")
    date_pattern = re.compile(r"date=(\d{4}-\d{2}-\d{2})")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if "centcom" not in key.lower():
                continue

            match = date_pattern.search(key)
            if match:
                date_str = match.group(1)
                if date_str not in objects_by_date:
                    objects_by_date[date_str] = {}
                objects_by_date[date_str][key] = obj["ETag"]

    if not objects_by_date:
        return None, {}

    most_recent_date = max(objects_by_date.keys())
    return most_recent_date, objects_by_date[most_recent_date]


def process_parquet_file(bucket, key):
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
            tmp_path = tmp.name

        s3.download_file(bucket, key, tmp_path)

        df = pd.read_parquet(tmp_path)
        csv_data = df.to_csv(index=False)

        # Max message size is 10000 chars. We use 9000 to leave room for formatting.
        chunk_size = 9000
        lines = csv_data.split("\n")

        chunks = []
        current_chunk = ""
        for line in lines:
            if not line:
                continue
            if len(current_chunk) + len(line) + 1 > chunk_size:
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = line + "\n"
            else:
                current_chunk += line + "\n"
        if current_chunk:
            chunks.append(current_chunk)

        filename = key.split("/")[-1]
        for i, chunk in enumerate(chunks):
            if len(chunks) > 1:
                message = (
                    f"File: `{filename}` (Part {i+1}/{len(chunks)})\n```csv\n{chunk}```"
                )
            else:
                message = f"File: `{filename}`\n```csv\n{chunk}```"
            print(f"Would have sent to ChatSurfer: {filename} Part {i+1}", flush=True)

        os.remove(tmp_path)
        print(
            f"[{now}] Successfully sent {len(chunks)} chunks to ChatSurfer from {key}",
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
                print(f"Would have sent to ChatSurfer: {key}", flush=True)
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
        f"Starting S3 daily dump. Base prefix: s3://{BUCKET_NAME}/{PREFIX} — polling every {POLL_INTERVAL_SECONDS}s",
        flush=True,
    )

    states = {}

    while True:
        most_recent_date, current_state = get_most_recent_day_objects(
            BUCKET_NAME, PREFIX
        )

        if most_recent_date:
            state_key = f"processed_{most_recent_date}"
            if state_key not in states:
                states[state_key] = current_state
                print(
                    f"[{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}] Found {len(current_state)} object(s) in centcom for most recent day ({most_recent_date}). Processing...",
                    flush=True,
                )

                parquet_keys = [
                    k for k in current_state.keys() if k.endswith(".parquet")
                ]
                if parquet_keys:

                    def files_iter():
                        for key in sorted(parquet_keys):
                            rel_path = key[len(PREFIX) :].lstrip("/")
                            yield key, rel_path

                    try:
                        pack(
                            files_iter(),
                            output_dir="/tmp/zips",
                            source_label="s3",
                            bucket=BUCKET_NAME,
                        )
                    except Exception as e:
                        print(f"Error packing parquets: {e}", flush=True)
            else:
                print(
                    f"[{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}] Already processed data for {most_recent_date}. Waiting for next day...",
                    flush=True,
                )
        else:
            print(
                f"[{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}] No date partitioned files found in centcom. Waiting...",
                flush=True,
            )

        time.sleep(POLL_INTERVAL_SECONDS)
