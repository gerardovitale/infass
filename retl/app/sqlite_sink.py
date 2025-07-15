import logging
import sqlite3
from typing import List
from typing import Optional

import pandas as pd
from sink import Sink
from sink import Transaction

logger = logging.getLogger(__name__)


class SQLiteSink(Sink):
    transaction_table_name = "retl_transactions"

    def __init__(
        self,
        db_path: str,
        table: str,
        is_incremental: Optional[bool] = None,
        index_columns: Optional[List[str]] = None,
        fts5_config: Optional[dict] = None,
    ):
        self.db_path = db_path
        self.table = table
        self.is_incremental = is_incremental
        self.index_columns = index_columns
        self.fts5_config = fts5_config
        self._last_transaction = None

    def write_data(self, df: pd.DataFrame) -> None:
        logger.info(f"Writing DataFrame to SQLite at {self.db_path}, table '{self.table}'")
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                params = {"if_exists": "replace", "index": False}
                if self.is_incremental and self.last_transaction:
                    logger.info("Incremental write mode is enabled")
                    params.update({"if_exists": "append"})
                if self.index_columns:
                    logger.info(f"Setting index columns: {self.index_columns}")
                    df = df.set_index(self.index_columns)
                    params.update({"index": True, "index_label": self.index_columns})
                logger.info(f"Writing DataFrame to SQLite with parameters: {params}")
                df.to_sql(self.table, conn, **params)
                logger.info("Write to SQLite completed")
                if self.fts5_config:
                    logger.info(f"Creating or refreshing FTS5 for table '{self.table}' with config: {self.fts5_config}")
                    self.create_or_refresh_fts5_table(conn, **self.fts5_config)
        except sqlite3.DatabaseError as err:
            logger.error(f"Database error during write_data: {err}")
            raise

    @property
    def last_transaction(self) -> Optional[Transaction]:
        if self._last_transaction is None and self.is_incremental:
            self._last_transaction = self.get_last_transaction_if_exist()
        return self._last_transaction

    def get_last_transaction_if_exist(self) -> Optional[Transaction]:
        def get_columns(cursor: sqlite3.Cursor) -> List[str]:
            return [column[0] for column in cursor.description]

        logger.info(f"Fetching last transaction from {self.transaction_table_name}")
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                cur = conn.cursor()
                cur.execute(
                    f"SELECT * FROM {self.transaction_table_name} "
                    f"WHERE destination_table = '{self.table}' "
                    f"ORDER BY occurred_at DESC LIMIT 1"
                )
                rows = cur.fetchall()
                return Transaction(**[dict(zip(get_columns(cur), row)) for row in rows][0]) if rows else None
        except sqlite3.OperationalError as err:
            logger.error(f"Failed to fetch last transaction from {self.transaction_table_name}: {err}")
            return None
        except sqlite3.DatabaseError as err:
            logger.error(f"Database error during get_last_transaction_if_exist: {err}")
            return None

    def record_transaction(self, txn: Transaction) -> None:
        logger.info(f"Writing Transaction to SQLite at {self.db_path}, table '{self.transaction_table_name}'")
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
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
                logger.info("Transaction writing completed")
        except sqlite3.DatabaseError as err:
            logger.error(f"Database error during record_transaction: {err}")
            raise

    def create_or_refresh_fts5_table(self, conn: sqlite3.Connection, id_column: str, columns: List[str]) -> None:
        fts_table = f"{self.table}_fts"
        columns_ddl = ", ".join(columns)
        logger.info(f"Creating or refreshing FTS5 table '{fts_table}' with columns: {columns}")
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {fts_table}")
        cur.execute(f"CREATE VIRTUAL TABLE {fts_table} USING fts5({id_column}, {columns_ddl})")
        columns_select = ", ".join([id_column] + columns)
        cur.execute(f"INSERT INTO {fts_table} SELECT {columns_select} FROM {self.table}")
        conn.commit()
        logger.info(f"FTS5 table '{fts_table}' created and populated successfully.")
