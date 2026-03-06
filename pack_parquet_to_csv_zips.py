#!/usr/bin/env python3
"""
pack_parquet_to_csv_zips.py
───────────────────────────
1. Walks an S3 bucket (or local directory) and finds every .parquet file.
2. Converts each one to CSV in memory (no temp files required).
3. Packs the CSVs into up to MAX_ZIPS zip archives, each ≤ MAX_ZIP_BYTES,
   preserving the original relative path inside the zip.

Transfer rules: max 6 zips, max 512 MB per zip. No per-zip file count limit.

Usage
─────
  # S3 source
  python pack_parquet_to_csv_zips.py \
      --source s3://my-bucket/optional/prefix \
      --output-dir ./zips

  # Local source
  python pack_parquet_to_csv_zips.py \
      --source /data/parquet-root \
      --output-dir ./zips

Dependencies
────────────
  pip install boto3 pandas pyarrow google-api-python-client google-auth-httplib2 google-auth-oauthlib
"""

import argparse
import io
import os
import sys
import zipfile
from pathlib import Path, PurePosixPath

import pandas as pd

# ── Tuneable limits ────────────────────────────────────────────────────────────
MAX_ZIPS = 1000
MAX_ZIP_BYTES = 500 * 1024 * 1024  # 500 MB limit for actual zip size
# ──────────────────────────────────────────────────────────────────────────────

# ── Google Drive API ───────────────────────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/drive.file"]


def authenticate_gdrive():
    import sys
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build

    creds = None
    token_path = os.environ.get("GDRIVE_TOKEN_PATH", "token.json")
    creds_path = os.environ.get("GDRIVE_CREDENTIALS_PATH", "credentials.json")

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
            creds = flow.run_local_server(port=0)
        try:
            with open(token_path, "w") as token:
                token.write(creds.to_json())
        except OSError as e:
            print(
                f"  ⚠  Could not write {token_path} (mounted read-only secret? continuing): {e}",
                file=sys.stderr,
            )

    return build("drive", "v3", credentials=creds)


def get_or_create_folder(service, folder_name, parent_id=None):
    query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    results = (
        service.files()
        .list(q=query, spaces="drive", fields="files(id, name)")
        .execute()
    )
    items = results.get("files", [])
    if not items:
        file_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
        }
        if parent_id:
            file_metadata["parents"] = [parent_id]
        folder = service.files().create(body=file_metadata, fields="id").execute()
        return folder.get("id")
    return items[0].get("id")


def get_target_folder_id(service, path):
    parts = path.strip("/").split("/")
    parent_id = None
    for part in parts:
        parent_id = get_or_create_folder(service, part, parent_id)
    return parent_id


def upload_to_drive(service, file_path, parent_folder_id):
    from googleapiclient.http import MediaFileUpload

    file_name = os.path.basename(file_path)
    file_metadata = {"name": file_name, "parents": [parent_folder_id]}
    media = MediaFileUpload(file_path, mimetype="application/zip", resumable=True)
    print(f"  ⬆ Uploading {file_name} to Google Drive...")
    service.files().create(body=file_metadata, media_body=media, fields="id").execute()
    print(f"  ✔ Finished uploading {file_name}")


# ──────────────────────────────────────────────────────────────────────────────


# ── Source abstraction ─────────────────────────────────────────────────────────


def is_s3(path: str) -> bool:
    return path.startswith("s3://")


def parse_s3(path: str):
    """Return (bucket, prefix) from s3://bucket/prefix."""
    stripped = path[5:]
    parts = stripped.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix


def list_s3_parquet(bucket: str, prefix: str):
    """Yield (s3_key, relative_path_str) for every .parquet object."""
    import boto3

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                rel = key[len(prefix) :].lstrip("/")
                yield key, rel


def read_s3_parquet_chunks(bucket: str, key: str):
    import boto3
    import pyarrow.parquet as pq
    import tempfile
    import os

    s3 = boto3.client("s3")

    fd, tmp_path = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)

    try:
        s3.download_file(bucket, key, tmp_path)
        with pq.ParquetFile(tmp_path) as pf:
            for batch in pf.iter_batches():
                yield batch.to_pandas()
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def list_local_parquet(root: str):
    """Yield (abs_path, relative_path_str) for every .parquet file."""
    root_path = Path(root)
    for p in sorted(root_path.rglob("*.parquet")):
        yield str(p), str(p.relative_to(root_path))


def read_local_parquet_chunks(path: str):
    import pyarrow.parquet as pq

    with pq.ParquetFile(path) as pf:
        for batch in pf.iter_batches():
            yield batch.to_pandas()


# ── CSV conversion ─────────────────────────────────────────────────────────────


def frame_to_csv_bytes(df: pd.DataFrame, header: bool) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=header)
    return buf.getvalue().encode("utf-8")


def rel_parquet_to_csv(rel: str) -> str:
    """Swap .parquet extension for .csv, keeping directory structure."""
    p = PurePosixPath(rel)
    return str(p.with_suffix(".csv"))


# ── Packing logic ──────────────────────────────────────────────────────────────


def pack(
    files_iter, output_dir: str, source_label: str, bucket: str = None, prefix: str = ""
):
    """
    files_iter yields (source_ref, relative_path_str).
    source_label: "s3" or "local".
    """
    os.makedirs(output_dir, exist_ok=True)

    from datetime import datetime

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("Initializing Google Drive API...")
    drive_service = authenticate_gdrive()

    # Prepend the parent folder to the specific S3 partition prefix
    drive_path = f"cosmic/iran-zips/{prefix}".strip("/")
    target_folder_id = get_target_folder_id(drive_service, drive_path)
    print(
        f"Initialized Drive API. Target Folder ID: {target_folder_id} (Path: {drive_path})"
    )

    zip_index = 1
    zip_count = 0
    zf = None
    zip_path = None
    summary = []

    def open_new_zip():
        nonlocal zip_index, zf, zip_path, zip_count
        if zf:
            zf.close()
            actual_size = os.path.getsize(zip_path)
            summary.append((zip_path, actual_size, zip_count))
            print(
                f"  ✔ Closed {os.path.basename(zip_path)}  "
                f"({zip_count} files, {actual_size/1024/1024:.1f} MB actual zip size)"
            )
            upload_to_drive(drive_service, zip_path, target_folder_id)

        if zip_index > MAX_ZIPS:
            sys.exit(
                f"\n✘  Reached MAX_ZIPS={MAX_ZIPS} and still have files remaining.\n"
                f"   Raise MAX_ZIPS at the top of the script and re-run."
            )
        zip_path = os.path.join(output_dir, f"transfer_{timestamp}_{zip_index:02d}.zip")
        zf = zipfile.ZipFile(
            zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6
        )
        zip_count = 0
        zip_index += 1

    open_new_zip()

    total_files = 0
    for source_ref, rel in files_iter:

        # We need to know the initial file sizes to estimate if we must roll-over first
        current_zip_size = 0 if not zf else zf.fp.tell()

        try:
            if source_label == "s3":
                chunks_iter = read_s3_parquet_chunks(bucket, source_ref)
            else:
                chunks_iter = read_local_parquet_chunks(source_ref)
        except Exception as e:
            print(f"  ⚠  Skipping {rel}: {e}", file=sys.stderr)
            continue

        csv_arc_path = rel_parquet_to_csv(rel)

        written_bytes = 0
        with zf.open(csv_arc_path, "w") as current_file:
            first_chunk = True
            for chunk_df in chunks_iter:
                csv_data = frame_to_csv_bytes(chunk_df, header=first_chunk)
                current_file.write(csv_data)
                written_bytes += len(csv_data)
                first_chunk = False

        zip_count += 1
        total_files += 1

        new_zip_size = zf.fp.tell()
        print(
            f"  + [zip {zip_index-1:02d}] {csv_arc_path}  (uncompressed: {written_bytes/1024:.1f} KB, zip is now {new_zip_size/1024/1024:.1f} MB)"
        )

        if new_zip_size >= MAX_ZIP_BYTES:
            open_new_zip()

    if zf:
        zf.close()
        if zip_count > 0:
            actual_size = os.path.getsize(zip_path)
            summary.append((zip_path, actual_size, zip_count))
            print(
                f"  ✔ Closed {os.path.basename(zip_path)}  "
                f"({zip_count} files, {actual_size/1024/1024:.1f} MB actual zip size)"
            )
            upload_to_drive(drive_service, zip_path, target_folder_id)
        else:
            os.remove(zip_path)

    print(f"\n{'═'*60}")
    print(f"Done. {total_files} parquet → CSV files packed and processed:\n")
    for zp, zb, zc in summary:
        print(
            f"  {os.path.basename(zp):30s}  {zc:4d} files  {zb/1024/1024:6.1f} MB actual zip size"
        )
    print()


# ── CLI ────────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Convert parquet → CSV and pack into ≤6 transfer zips (≤512 MB each)."
    )
    parser.add_argument(
        "--source",
        required=True,
        help="S3 URI (s3://bucket/prefix) or local directory path.",
    )
    parser.add_argument(
        "--output-dir",
        default="./zips",
        help="Directory to write transfer_NN.zip files into. (default: ./zips)",
    )
    args = parser.parse_args()

    print(f"Source      : {args.source}")
    print(f"Output dir  : {args.output_dir}")
    print(
        f"Max zips    : {MAX_ZIPS}  |  Max size/zip : {MAX_ZIP_BYTES//1024//1024} MB\n"
    )

    if is_s3(args.source):
        bucket, prefix = parse_s3(args.source)
        print(f"Mode: S3  bucket={bucket}  prefix='{prefix}'\n")
        files_iter = list_s3_parquet(bucket, prefix)
        pack(files_iter, args.output_dir, source_label="s3", bucket=bucket)
    else:
        print(f"Mode: local  root={args.source}\n")
        files_iter = list_local_parquet(args.source)
        pack(files_iter, args.output_dir, source_label="local")


if __name__ == "__main__":
    main()
