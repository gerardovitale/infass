import logging
import sqlite3
from abc import ABC
from typing import List

import pandas as pd
from google.cloud import bigquery
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class Transaction(BaseModel):
    data_source_table: str
    destination_table: str
    occurred_at: str
    min_date: str
    max_date: str


class Sink(ABC):
    def fetch_data(self) -> pd.DataFrame:
        raise NotImplementedError()

    def write_data(self, df: pd.DataFrame) -> None:
        raise NotImplementedError()


class BigQuerySink(Sink):
    def __init__(self, project_id: str, dataset_id: str, table: str, client: bigquery.Client):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table = table
        self.client = client

    def fetch_data(self) -> pd.DataFrame:
        logger.info(f"Fetching data from BigQuery table: {self.project_id}.{self.dataset_id}.{self.table}")
        query = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.{self.table}`"
        df = self.client.query(query).to_dataframe()
        logger.info(f"Fetched {len(df)} rows from BigQuery")
        logger.info(f"DataFrame size: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        return df


class SQLiteSink(Sink):
    transaction_table_name = "retl_transactions"

    def __init__(self, db_path: str, table: str, index_columns: List[str] = None):
        self.db_path = db_path
        self.table = table
        self.index_columns = index_columns

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

    def get_last_transaction(self):
        def get_columns(cursor: sqlite3.Cursor) -> List[str]:
            return [column[0] for column in cursor.description]

        logger.info(f"Fetching last transaction from {self.transaction_table_name}")
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {self.transaction_table_name} ORDER BY occurred_at DESC LIMIT 1")
        rows = cur.fetchall()
        return Transaction(**[dict(zip(get_columns(cur), row)) for row in rows][0])

    def record_transaction(self, txn: Transaction) -> None:
        logger.info(f"Writing Transaction to SQLite at {self.db_path}, table '{self.transaction_table_name}'")
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.transaction_table_name}
            (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data_source_table TEXT,
                destination_table REAL,
                occurred_at TEXT,
                min_date TEXT,
                max_date TEXT
            );
            """
        )
        record = (txn.data_source_table, txn.destination_table, txn.occurred_at, txn.min_date, txn.max_date)
        logger.info(f"Writing Transaction: {record}")
        cur.execute(
            f"INSERT INTO {self.transaction_table_name} "
            "(data_source_table, destination_table, occurred_at, min_date, max_date) "
            "VALUES (?, ?, ?, ?, ?);",
            record,
        )
        conn.commit()
        conn.close()
        logger.info("Transaction writing completed")
