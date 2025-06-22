from __future__ import annotations

import functools
import logging
import time
from typing import Tuple

import pandas as pd

logger = logging.getLogger(__name__)


def timeit(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        logger.info(f"Function {func.__name__!r} executed in {end - start:.4f} seconds")
        return result

    return wrapper


def get_min_max_dates(d: pd.DataFrame) -> Tuple[str, str] | Tuple[None, None]:
    if "date" in d.columns:
        min_dt, max_dt = d["date"].min(), d["date"].max()

        try:
            min_date = pd.to_datetime(min_dt).date().isoformat()
            max_date = pd.to_datetime(max_dt).date().isoformat()
            return min_date, max_date

        except (ValueError, TypeError):
            logger.error(f"Invalid date values: min={min_dt}, max={max_dt}")
            return None, None

    return None, None
