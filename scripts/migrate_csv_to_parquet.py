import argparse
import sys
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage


PARQUET_COMPRESSION = "snappy"
ESTIMATED_COMPRESSION_RATIO = 0.3  # Parquet is typically ~30% the size of CSV


def list_csv_blobs(client: storage.Client, bucket_name: str, prefix: str):
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=f"{prefix}/")
    return [b for b in blobs if b.name.endswith(".csv")]


def format_size(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} B"
    if size_bytes < 1024**2:
        return f"{size_bytes / 1024:.1f} KB"
    return f"{size_bytes / 1024 ** 2:.1f} MB"


def do_list(client: storage.Client, bucket_name: str, prefix: str):
    csv_blobs = list_csv_blobs(client, bucket_name, prefix)
    if not csv_blobs:
        print("No CSV files found.")
        return

    total_size = 0
    print(f"{'Blob Name':<60} {'Size':>10}")
    print("-" * 72)
    for blob in csv_blobs:
        print(f"{blob.name:<60} {format_size(blob.size):>10}")
        total_size += blob.size

    estimated_parquet = int(total_size * ESTIMATED_COMPRESSION_RATIO)
    print("-" * 72)
    print(f"{'Total CSV files:':<60} {len(csv_blobs):>10}")
    print(f"{'Total CSV size:':<60} {format_size(total_size):>10}")
    pct = int(ESTIMATED_COMPRESSION_RATIO * 100)
    label = f"Estimated Parquet size (~{pct}%):"
    print(f"{label:<60} {format_size(estimated_parquet):>10}")
    print(f"{'Estimated savings:':<60} {format_size(total_size - estimated_parquet):>10}")


def convert_csv_to_parquet(csv_bytes: bytes) -> BytesIO:
    df = pd.read_csv(BytesIO(csv_bytes))
    buffer = BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, buffer, compression=PARQUET_COMPRESSION)
    buffer.seek(0)
    return buffer


def do_migrate(client: storage.Client, bucket_name: str, prefix: str, dry_run: bool):
    csv_blobs = list_csv_blobs(client, bucket_name, prefix)
    if not csv_blobs:
        print("No CSV files found.")
        return

    bucket = client.bucket(bucket_name)
    total_csv_size = 0
    total_parquet_size = 0
    migrated = 0
    failed = 0

    for blob in csv_blobs:
        parquet_name = blob.name.rsplit(".csv", 1)[0] + ".parquet"
        csv_size = blob.size

        if dry_run:
            estimated = int(csv_size * ESTIMATED_COMPRESSION_RATIO)
            print(f"[DRY RUN] {blob.name} ({format_size(csv_size)}) -> {parquet_name} (~{format_size(estimated)})")
            total_csv_size += csv_size
            total_parquet_size += estimated
            migrated += 1
            continue

        try:
            csv_bytes = blob.download_as_bytes()
            parquet_buffer = convert_csv_to_parquet(csv_bytes)
            parquet_size = parquet_buffer.getbuffer().nbytes

            parquet_blob = bucket.blob(parquet_name)
            parquet_blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")
            blob.delete()

            total_csv_size += csv_size
            total_parquet_size += parquet_size
            migrated += 1
            print(f"Migrated {blob.name} ({format_size(csv_size)}) -> {parquet_name} ({format_size(parquet_size)})")
        except Exception as e:
            failed += 1
            print(f"FAILED {blob.name}: {e}", file=sys.stderr)

    print()
    prefix_label = "[DRY RUN] " if dry_run else ""
    print(f"{prefix_label}Migrated: {migrated}, Failed: {failed}")
    print(f"{prefix_label}Total CSV size:     {format_size(total_csv_size)}")
    print(f"{prefix_label}Total Parquet size:  {format_size(total_parquet_size)}")
    if total_csv_size > 0:
        savings = total_csv_size - total_parquet_size
        print(f"{prefix_label}Savings:             {format_size(savings)} ({savings * 100 // total_csv_size}%)")


def main():
    parser = argparse.ArgumentParser(description="Migrate CSV files to Parquet in GCS")
    parser.add_argument("--bucket", required=True, help="GCS bucket name")
    parser.add_argument("--prefix", required=True, help="Blob prefix (e.g., merc, carr)")

    action = parser.add_mutually_exclusive_group()
    action.add_argument("--list", action="store_true", default=True, dest="list_mode", help="List CSV files (default)")
    action.add_argument("--migrate", action="store_true", help="Convert CSVs to Parquet and delete originals")

    parser.add_argument(
        "--dry-run", action="store_true", help="With --migrate, show what would happen without making changes"
    )
    args = parser.parse_args()

    client = storage.Client()

    if args.migrate:
        do_migrate(client, args.bucket, args.prefix, args.dry_run)
    else:
        do_list(client, args.bucket, args.prefix)


if __name__ == "__main__":
    main()
