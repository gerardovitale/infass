import sqlite3

import pandas as pd
from google.cloud import storage

BUCKET_NAME = "infass-sqlite-bucket"
OBJECT_NAME = "infass-sqlite-api.db"
LOCAL_PATH = "../data/infass-sqlite-api.db"


def download_from_gcs(bucket_name: str, object_name: str, local_path: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.download_to_filename(local_path)
    print(f"✅ Downloaded gs://{bucket_name}/{object_name} to {local_path}")


def inspect_sqlite(db_path: str):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # List all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    # Show top 5 rows per table
    if tables:
        for table in tables:
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


if __name__ == "__main__":
    # download_from_gcs(BUCKET_NAME, OBJECT_NAME, LOCAL_PATH)
    inspect_sqlite(LOCAL_PATH)
