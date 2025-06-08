import logging
import os
import sqlite3

import pandas as pd
from google.cloud import bigquery
from google.cloud import storage

logging.basicConfig(
    format="%(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)


def fetch_from_bigquery(project_id: str, dataset_id: str, table_name: str) -> pd.DataFrame:
    logger.info(f"Fetching data from BigQuery: {project_id}.{dataset_id}.{table_name}")
    client = bigquery.Client(project=project_id)
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_name}`"
    df = client.query(query).to_dataframe()
    logger.info(f"Fetched {len(df)} rows from BigQuery")
    return df


def write_to_sqlite(df: pd.DataFrame, db_path: str, table_name: str = "products") -> None:
    logger.info(f"Writing DataFrame to SQLite at {db_path}, table '{table_name}'")
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    logger.info("Write to SQLite completed")


def upload_to_gcs(bucket_name: str, blob_name: str, file_path: str) -> None:
    logger.info(f"Uploading {file_path} to GCS bucket '{bucket_name}' as '{blob_name}'")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)
    logger.info("Upload to GCS completed")


def main():
    logger.info("Starting Reversed ETL process")
    project_id = os.environ["PROJECT_ID"]
    dataset_id = os.environ["DATASET_ID"]
    table_name = os.environ["BQ_TABLE"]
    bucket_name = os.environ["GCS_BUCKET"]
    blob_name = os.environ["GCS_OBJECT"]
    local_path = "/tmp/api.db"

    logger.info("Step 1: Fetching data from BigQuery")
    df = fetch_from_bigquery(project_id, dataset_id, table_name)
    logger.info("Step 2: Writing data to SQLite")
    write_to_sqlite(df, local_path)
    logger.info("Step 3: Uploading SQLite DB to GCS")
    upload_to_gcs(bucket_name, blob_name, local_path)
    logger.info("ETL process completed successfully")


if __name__ == "__main__":
    main()
