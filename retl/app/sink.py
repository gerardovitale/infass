import logging
from abc import ABC
from typing import Optional

import pandas as pd
from sqlmodel import Field
from sqlmodel import SQLModel

logger = logging.getLogger(__name__)


class Transaction(SQLModel, table=True):
    __tablename__ = "retl_transactions"
    __table_args__ = {"extend_existing": True}

    id: Optional[int] = Field(default=None, primary_key=True)
    data_source_table: str
    destination_table: str
    occurred_at: str
    min_date: Optional[str] = None
    max_date: Optional[str] = None


class Sink(ABC):
    def fetch_data(self) -> pd.DataFrame:
        raise NotImplementedError()

    def write_data(self, df: pd.DataFrame) -> None:
        raise NotImplementedError()
