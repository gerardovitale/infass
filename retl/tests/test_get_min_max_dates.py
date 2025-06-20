from datetime import datetime

import pandas as pd
from main import get_min_max_dates


def test_min_max_dates_with_date_objects():
    df = pd.DataFrame(
        {
            "date": [
                pd.Timestamp("2023-01-01").date(),
                pd.Timestamp("2023-01-03").date(),
                pd.Timestamp("2023-01-02").date(),
            ]
        }
    )
    assert get_min_max_dates(df) == ("2023-01-01", "2023-01-03")


def test_min_max_dates_with_timestamp_objects():
    df = pd.DataFrame({"date": [pd.Timestamp("2023-01-01"), pd.Timestamp("2023-01-03"), pd.Timestamp("2023-01-02")]})
    assert get_min_max_dates(df) == ("2023-01-01", "2023-01-03")


def test_min_max_dates_with_datetime_objects():
    df = pd.DataFrame({"date": [datetime(2023, 1, 1), datetime(2023, 1, 3), datetime(2023, 1, 2)]})
    assert get_min_max_dates(df) == ("2023-01-01", "2023-01-03")


def test_min_max_dates_with_str_dates():
    df = pd.DataFrame({"date": ["2023-01-01", "2023-01-03", "2023-01-02"]})
    assert get_min_max_dates(df) == ("2023-01-01", "2023-01-03")


def test_min_max_dates_with_invalid_str_dates():
    df = pd.DataFrame({"date": ["not-a-date", "2023-01-03", "2023-01-02"]})
    assert get_min_max_dates(df) == (None, None)


def test_min_max_dates_missing_date_column():
    df = pd.DataFrame({"other": [1, 2, 3]})
    assert get_min_max_dates(df) == (None, None)
