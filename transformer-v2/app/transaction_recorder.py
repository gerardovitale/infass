from __future__ import annotations

import logging
import os
import sqlite3
from abc import ABC
from abc import abstractmethod
from datetime import datetime
from typing import Optional

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class TransactionRecorder(ABC):
    @abstractmethod
    def record(self, min_date: str | None, max_date: str | None) -> None:
        raise NotImplementedError()

    @abstractmethod
    def get_last_transaction_if_exists(self) -> Transaction | None:
        raise NotImplementedError()


class Transaction(BaseModel):
    product: str
    data_source_table: str
    destination_table: str
    occurred_at: str
    min_date: Optional[str]
    max_date: Optional[str]


class TxnRecSQLite(TransactionRecorder):
    def __init__(self, db_path: str, product: str, data_source: str, destination: str):
        logger.info(
            f"Initializing TransactionRecorder with db_path: {db_path}, product: {product}, "
            f"data_source: {data_source}, destination: {destination}"
        )
        self.db_path = db_path
        self.product = product
        self.data_source = data_source
        self.destination = destination
        self._validate_db()

    def _validate_db(self) -> None:
        logger.info("Validating SQLite database")
        if not os.path.exists(self.db_path):
            logger.error(f"Database file does not exist: {self.db_path}")
            raise Exception(f"Database file does not exist: {self.db_path}")
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
        except sqlite3.DatabaseError as e:
            logger.error(f"Invalid SQLite database: {self.db_path}")
            raise Exception(f"Invalid SQLite database: {self.db_path}") from e

    def record(self, min_date: str | None, max_date: str | None) -> None:
        logger.info(f"Recording transaction for product: {self.product}")
        tnx = Transaction(
            product=self.product,
            data_source_table=self.data_source,
            destination_table=self.destination,
            occurred_at=datetime.now().isoformat(timespec="seconds"),
            min_date=min_date,
            max_date=max_date,
        )
        logger.info(f"Transaction to be recorded: {tnx}")
        pass

    def get_last_transaction_if_exists(self) -> Transaction | None:
        logger.info("Fetching last transaction")
        pass
