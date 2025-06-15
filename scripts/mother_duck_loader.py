from __future__ import annotations

import logging
import os
from io import StringIO
from typing import Iterator
from typing import List

import duckdb
import pandas as pd
import pyarrow as pa
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class GCS:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.client = storage.Client()

    def _list_blobs(self) -> List[storage.Blob]:
        return sorted(self.client.list_blobs(self.bucket_name, prefix="merc/"), key=lambda b: b.name, reverse=True)

    def _read_blob(self, blob_name: str) -> str:
        bucket = self.client.bucket(self.bucket_name)
        blob = bucket.blob(blob_name)
        with blob.open("r") as f:
            return f.read()

    def iter_dataframes(self, schema: List[str], limit: int | None = None) -> Iterator[pd.DataFrame]:
        logging.info(f"Listing objects from {self.bucket_name}")
        blobs = self._list_blobs()
        if limit:
            if limit == -1:
                blobs = blobs[:1]
                logging.info("Reading only the most recent object")
            else:
                blobs = blobs[:limit]
                logging.info(f"Setting limit to {limit}")
        for b in blobs:
            logging.info(f"Reading {b.name}")
            string_csv_data = self._read_blob(b.name)
            csv_data = StringIO(string_csv_data)
            yield pd.read_csv(csv_data)[schema]

    def get_data_as_pd(self, schema: List[str], limit: int | None = None) -> pd.DataFrame:
        df_list = list(self.iter_dataframes(schema, limit))
        if not df_list:
            return pd.DataFrame(columns=schema)
        df = pd.concat(df_list, ignore_index=True)
        logging.info(f"Loaded {len(df)} rows, DataFrame size: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        return df


class MotherDuckLoadingBuffer:
    def __init__(
        self,
        duckdb_schema: str,
        pyarrow_schema: pa.Schema,
        database_name: str,
        table_name: str,
        destination: str = "local",
        chunk_size: int = 100_000,
    ):
        self.duckdb_schema = duckdb_schema
        self.pyarrow_schema = pyarrow_schema
        self.database_name = database_name
        self.table_name = table_name
        self.total_inserted = 0
        self.chunk_size = chunk_size
        self.conn = self.initialize_connection(destination, duckdb_schema)

    def initialize_connection(self, destination: str, sql: str) -> duckdb.DuckDBPyConnection:
        if destination == "md":
            logging.info("Connecting to MotherDuck...")
            if not os.environ.get("MOTHERDUCK_TOKEN"):
                raise ValueError("MotherDuck token is required. Set the environment variable 'MOTHERDUCK_TOKEN'.")
            conn = duckdb.connect("md:")
            logging.info(f"Creating/using database {self.database_name}")
            conn.execute(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")
            conn.execute(f"USE {self.database_name}")
        else:
            conn = duckdb.connect(database=f"{self.database_name}.db")
        conn.execute(sql)
        return conn

    def insert(self, table: pa.Table):
        logging.info(f"Inserting {table.num_rows} rows into {self.table_name} in {self.database_name}")
        total_rows = table.num_rows
        for batch_start in range(0, total_rows, self.chunk_size):
            batch_end = min(batch_start + self.chunk_size, total_rows)
            chunk = table.slice(batch_start, batch_end - batch_start)
            self.insert_chunk(chunk)
            logging.info(f"Inserted rows {batch_start} to {batch_end}")
        self.total_inserted += total_rows
        logging.info(f"Total inserted: {self.total_inserted} rows")

    def insert_chunk(self, chunk: pa.Table):
        self.conn.register("buffer_table", chunk)
        insert_query = f"INSERT INTO {self.table_name} SELECT * FROM buffer_table"
        self.conn.execute(insert_query)
        self.conn.unregister("buffer_table")


def main():
    logging.info("Starting MotherDuck loading process...")
    ingestion_schema = [
        "date",
        "name",
        "size",
        "category",
        "original_price",
        "discount_price",
        "image_url",
    ]
    gcs = GCS(bucket_name="infass-merc")
    data = gcs.get_data_as_pd(ingestion_schema, limit=7)
    if data.empty:
        logging.warning("No data found to load.")
        return
    logging.info("Transforming data to PyArrow Table")
    arrow_table = pa.Table.from_pandas(data)
    pyarrow_schema = pa.Schema.from_pandas(data)
    duckdb_schema = """
        CREATE OR REPLACE TABLE raw_merc
        (
            date DATE,
            name VARCHAR,
            size VARCHAR,
            category VARCHAR,
            original_price VARCHAR,
            discount_price VARCHAR,
            image_url VARCHAR
        )
    """
    logging.info(f"Dumping {arrow_table.num_rows} rows")
    loader = MotherDuckLoadingBuffer(
        duckdb_schema=duckdb_schema,
        pyarrow_schema=pyarrow_schema,
        database_name="my_db",
        table_name="raw_merc",
        destination="md",
        chunk_size=100,
    )
    loader.insert(arrow_table)
    logging.info("MotherDuck loading process completed successfully.")


if __name__ == "__main__":
    main()
