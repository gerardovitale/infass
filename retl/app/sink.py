import logging
from abc import ABC
from typing import Optional

import pandas as pd
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class Transaction(BaseModel):
    data_source_table: str
    destination_table: str
    occurred_at: str
    min_date: Optional[str]
    max_date: Optional[str]


class Sink(ABC):
    def fetch_data(self) -> pd.DataFrame:
        raise NotImplementedError()

    def write_data(self, df: pd.DataFrame) -> None:
        raise NotImplementedError()
