from __future__ import annotations

from typing import Tuple

import pandas as pd


def get_min_max_dates(d: pd.DataFrame) -> Tuple[str, str] | Tuple[None, None]:
    if "date" in d.columns:
        min_dt, max_dt = d["date"].min(), d["date"].max()

        try:
            min_date = pd.to_datetime(min_dt).date().isoformat()
            max_date = pd.to_datetime(max_dt).date().isoformat()
            return min_date, max_date

        except (ValueError, TypeError):
            return None, None

    return None, None
