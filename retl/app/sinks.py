import logging
import sqlite3
from abc import ABC
from typing import List

import pandas as pd
from google.cloud import bigquery
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class Sink(ABC):
    def fetch_data(self) -> pd.DataFrame:
        raise NotImplementedError()

    def write_data(self, df: pd.DataFrame) -> None:
        raise NotImplementedError()


class BigQuerySink(BaseModel, Sink):
    project_id: str
    dataset_id: str
    table: str

    def fetch_data(self) -> pd.DataFrame:
        logger.info(f"Fetching data from BigQuery table: {self.project_id}.{self.dataset_id}.{self.table}")
        client = bigquery.Client(project=self.project_id)
        query = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.{self.table}`"
        df = client.query(query).to_dataframe()
        logger.info(f"Fetched {len(df)} rows from BigQuery")
        logger.info(f"DataFrame size: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        return df


class SQLiteSink(BaseModel, Sink):
    db_path: str
    table: str
    index_columns: List[str] = None

    def write_data(self, df: pd.DataFrame) -> None:
        logger.info(f"Writing DataFrame to SQLite at {self.db_path}, table '{self.table}'")
        conn = sqlite3.connect(self.db_path)
        params = {"if_exists": "replace", "index": False}
        if self.index_columns:
            df = df.set_index(self.index_columns)
            params.update({"index": True, "index_label": self.index_columns})
        df.to_sql(self.table, conn, **params)
        conn.close()
        logger.info("Write to SQLite completed")
