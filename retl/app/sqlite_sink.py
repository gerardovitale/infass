import logging
from typing import List
from typing import Optional

import pandas as pd
import sqlalchemy
from sink import Sink
from sink import Transaction
from sqlmodel import create_engine
from sqlmodel import select
from sqlmodel import Session
from sqlmodel import SQLModel

logger = logging.getLogger(__name__)


def _set_wal_mode(dbapi_conn, connection_record):
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.close()


class SQLiteSink(Sink):

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
        self.engine = create_engine(f"sqlite:///{db_path}")
        sqlalchemy.event.listen(self.engine, "connect", _set_wal_mode)
        SQLModel.metadata.create_all(self.engine)

    def write_data(self, df: pd.DataFrame) -> None:
        logger.info(f"Writing DataFrame to SQLite at {self.db_path}, table '{self.table}'")
        try:
            with self.engine.connect() as conn:
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
                conn.commit()
                logger.info("Write to SQLite completed")
                if self.fts5_config:
                    logger.info(f"Creating or refreshing FTS5 for table '{self.table}' with config: {self.fts5_config}")
                    self.create_or_refresh_fts5_table(conn, **self.fts5_config)
        except Exception as err:
            logger.error(f"Database error during write_data: {err}")
            raise

    @property
    def last_transaction(self) -> Optional[Transaction]:
        if self._last_transaction is None and self.is_incremental:
            self._last_transaction = self.get_last_transaction_if_exist()
        return self._last_transaction

    def get_last_transaction_if_exist(self) -> Optional[Transaction]:
        logger.info("Fetching last transaction from retl_transactions")
        try:
            with Session(self.engine) as session:
                statement = (
                    select(Transaction)
                    .where(Transaction.destination_table == self.table)
                    .order_by(Transaction.occurred_at.desc())
                    .limit(1)
                )
                return session.exec(statement).first()
        except Exception as err:
            logger.error(f"Failed to fetch last transaction: {err}")
            return None

    def record_transaction(self, txn: Transaction) -> None:
        logger.info(f"Writing Transaction to SQLite at {self.db_path}, table 'retl_transactions'")
        try:
            with Session(self.engine) as session:
                session.add(txn)
                session.commit()
                logger.info("Transaction writing completed")
        except Exception as err:
            logger.error(f"Database error during record_transaction: {err}")
            raise

    def create_or_refresh_fts5_table(self, conn, id_column: str, columns: List[str]) -> None:
        fts_table = f"{self.table}_fts"
        columns_ddl = ", ".join(columns)
        logger.info(f"Creating or refreshing FTS5 table '{fts_table}' with columns: {columns}")
        conn.execute(sqlalchemy.text(f"DROP TABLE IF EXISTS {fts_table}"))
        conn.execute(sqlalchemy.text(f"CREATE VIRTUAL TABLE {fts_table} USING fts5({id_column}, {columns_ddl})"))
        columns_select = ", ".join([id_column] + columns)
        conn.execute(sqlalchemy.text(f"INSERT INTO {fts_table} SELECT {columns_select} FROM {self.table}"))
        conn.commit()
        logger.info(f"FTS5 table '{fts_table}' created and populated successfully.")
