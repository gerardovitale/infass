import os
import sqlite3

import pandas as pd
from google.cloud import storage
from google.oauth2.service_account import Credentials

BUCKET_NAME = "infass-sqlite-bucket"
PARAMS = [
    {
        "object_name": "infass-test-sqlite-api.db",
        "local_path": "data/infass-test-sqlite-api.db",
    },
    {
        "object_name": "infass-sqlite-api.db",
        "local_path": "data/infass-sqlite-api.db",
    },
]


def get_gcp_credentials() -> Credentials:
    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "retl/retl-creds.json")
    if not os.path.exists(cred_path):
        raise FileNotFoundError(
            f"Google Cloud credentials file not found at `{cred_path}`. "
            "Set GOOGLE_APPLICATION_CREDENTIALS or place the file at that path."
        )
    return Credentials.from_service_account_file(cred_path)


def get_client() -> storage.Client:
    print(f"Connecting to `{BUCKET_NAME}`")
    try:
        return storage.Client()
    except Exception as e:
        print(f"Default storage.Client() failed: {e}. Trying with explicit credentials.")
        return storage.Client(credentials=get_gcp_credentials())


def download_from_gcs(bucket_name: str, object_name: str, local_path: str) -> None:
    client = get_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.download_to_filename(local_path)
    print(f"✅ Downloaded `gs://{bucket_name}/{object_name}` to `{local_path}`")


def inspect_sqlite(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # List all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    # Show top 5 rows per table
    if tables:
        for table in tables:
            if "fts_" in table[0]:
                print(f"Skipping FTS table: `{table[0]}`")
                continue

            print(f"🔍 Preview of `{table[0]}`:")
            cursor.execute(f"SELECT * FROM {table[0]} LIMIT 5;")
            column_names = [column[0] for column in cursor.description]
            print("Cursor description: ", column_names)
            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=column_names)
            print(f"DataFrame shape: {df.shape}")
            print("DataFrame preview:")
            print(df.head().to_string(index=False), "\n")
            df.info()
            print("\n" + "=" * 100 + "\n")

    conn.close()
    print("✅ SQLite inspection completed.")


if __name__ == "__main__":
    for param in PARAMS:
        object_name = param["object_name"]
        local_path = param["local_path"]

        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        download_from_gcs(BUCKET_NAME, object_name, local_path)
        inspect_sqlite(local_path)
