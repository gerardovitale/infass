from __future__ import annotations

import logging
import os
import sqlite3
from abc import ABC
from abc import abstractmethod
from datetime import datetime
from typing import Optional

from sqlmodel import create_engine
from sqlmodel import Field
from sqlmodel import select
from sqlmodel import Session
from sqlmodel import SQLModel

logger = logging.getLogger(__name__)


class TransactionRecorder(ABC):
    @abstractmethod
    def record(self, min_date: str | None, max_date: str | None) -> None:
        raise NotImplementedError()

    @abstractmethod
    def get_last_txn_if_exists(self) -> Transaction | None:
        raise NotImplementedError()


class Transaction(SQLModel, table=True):
    __tablename__ = "transactions"
    __table_args__ = {"extend_existing": True}

    id: Optional[int] = Field(default=None, primary_key=True)
    product: str
    data_source: str
    destination: str
    occurred_at: str
    min_date: Optional[str] = None
    max_date: Optional[str] = None


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
        self.engine = create_engine(f"sqlite:///{db_path}")
        SQLModel.metadata.create_all(self.engine)

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

    def create_txn_obj(self, min_date: str | None, max_date: str | None) -> Transaction:
        logger.info(f"Creating transaction for product: {self.product}")
        return Transaction(
            product=self.product,
            data_source=self.data_source,
            destination=self.destination,
            occurred_at=datetime.now().isoformat(timespec="seconds"),
            min_date=min_date,
            max_date=max_date,
        )

    def record(self, min_date: str | None, max_date: str | None) -> None:
        logger.info(f"Recording transaction for product: {self.product}")
        txn = self.create_txn_obj(min_date, max_date)
        logger.info(f"Transaction to be recorded: {txn}")
        with Session(self.engine) as session:
            session.add(txn)
            session.commit()
        logger.info("Transaction recorded successfully")

    def get_last_txn_if_exists(self) -> Transaction | None:
        logger.info("Fetching last transaction")
        with Session(self.engine) as session:
            statement = (
                select(Transaction)
                .where(Transaction.product == self.product)
                .where(Transaction.data_source == self.data_source)
                .where(Transaction.destination == self.destination)
                .order_by(Transaction.id.desc())
                .limit(1)
            )
            txn = session.exec(statement).first()
            if txn:
                logger.info(f"Last transaction found: {txn}")
            return txn
