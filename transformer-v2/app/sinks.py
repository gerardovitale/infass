import logging
import re
from abc import ABC
from datetime import date
from datetime import datetime
from io import BytesIO
from io import StringIO
from typing import Iterable
from typing import Optional

import pandas as pd
from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.bigquery import LoadJobConfig
from google.cloud.storage import Blob as StorageBlob
from google.cloud.storage import Client as StorageClient

DATE_PATTERN_FILE_NAME = re.compile(r"\d{4}-\d{2}-\d{2}")

logger = logging.getLogger(__name__)


class Sink(ABC):
    def fetch_data(self, last_txn_date: Optional[str] = None) -> pd.DataFrame:
        raise NotImplementedError()

    def write_data(self, df: pd.DataFrame) -> None:
        raise NotImplementedError()


class Storage(Sink):
    def __init__(self, bucket_name: str, prefix: str):
        logger.info(f"Initializing Storage Sink for bucket: {bucket_name} with prefix: {prefix}")
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.client = StorageClient()

    def fetch_data(self, last_txn_date: Optional[str] = None) -> pd.DataFrame:
        blobs_iter = self.client.list_blobs(self.bucket_name, prefix=self.prefix)
        if last_txn_date:
            logger.info(f"Last transaction passed, fetching data incrementally with: last_txn_date: {last_txn_date}")
            blobs_iter = self.filter_by_date(blobs_iter, last_txn_date)
        return self.build_dataframe(blobs_iter)

    def build_dataframe(self, blobs: Iterable[StorageBlob]) -> pd.DataFrame:
        df_list = []
        for b in blobs:
            df_list.append(self.read_blob(b.name))

        logger.info(f"Read {len(df_list)} objects")
        if not df_list:
            logger.info("No matching blobs found. Returning empty DataFrame.")
            return pd.DataFrame()

        df = pd.concat(df_list, ignore_index=True)
        logger.info(f"DataFrame size: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        return df

    def read_blob(self, blob_name: str) -> pd.DataFrame:
        logger.info(f"Reading {blob_name}")
        bucket = self.client.bucket(self.bucket_name)
        blob = bucket.blob(blob_name)
        if blob_name.endswith(".parquet"):
            with blob.open("rb") as f:
                return pd.read_parquet(BytesIO(f.read()))
        with blob.open("r") as f:
            return pd.read_csv(StringIO(f.read()))

    def filter_by_date(self, files: Iterable[StorageBlob], given_date: str):
        given_dt = datetime.strptime(given_date, "%Y-%m-%d").date()
        return filter(lambda f: (d := self.extract_date(f.name)) and d > given_dt, files)

    @staticmethod
    def extract_date(filename: str) -> Optional[date]:
        match = DATE_PATTERN_FILE_NAME.search(filename)
        if not match:
            return None
        try:
            return datetime.strptime(match.group(), "%Y-%m-%d").date()
        except ValueError:
            return None


class BigQuery(Sink):
    def __init__(self, table_ref: str, write_config: dict):
        logger.info(f"Initializing BigQuery Sink for table: {table_ref}")
        self.table_ref = table_ref
        self.client = BigQueryClient()
        self.write_config = write_config

    def write_data(self, df: pd.DataFrame) -> None:
        job_config = LoadJobConfig(**self.write_config, autodetect=False)
        try:
            job = self.client.load_table_from_dataframe(df, self.table_ref, job_config=job_config)
            job.result()  # Waits for the job to complete
            logger.info(f"Data successfully written to {self.table_ref}.")
        except Exception as e:
            logger.error(f"Error writing to BigQuery: {e}")
            raise
