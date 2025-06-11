import logging
import sqlite3
from abc import ABC

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
        return df


class SQLiteSink(BaseModel, Sink):
    db_path: str
    table: str

    def write_data(self, df: pd.DataFrame) -> None:
        logger.info(f"Writing DataFrame to SQLite at {self.db_path}, table '{self.table}'")
        conn = sqlite3.connect(self.db_path)
        df.to_sql(self.table, conn, if_exists="replace", index=False)
        conn.close()
        logger.info("Write to SQLite completed")
