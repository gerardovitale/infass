import pandas as pd
from app.utils import get_min_max_dates


def test_get_min_max_dates_valid_df():
    df = pd.DataFrame({"date": ["2023-01-01", "2023-01-05", "2023-01-03"]})
    min_date, max_date = get_min_max_dates(df)
    assert min_date == "2023-01-01"
    assert max_date == "2023-01-05"


def test_get_min_max_dates_missing_date_column():
    df = pd.DataFrame({"value": [1, 2, 3]})
    min_date, max_date = get_min_max_dates(df)
    assert min_date is None
    assert max_date is None


def test_get_min_max_dates_invalid_input_type():
    min_date, max_date = get_min_max_dates([1, 2, 3])
    assert min_date is None
    assert max_date is None


def test_get_min_max_dates_invalid_date_values():
    df = pd.DataFrame({"date": ["not-a-date", "not-a-date", ""]})
    min_date, max_date = get_min_max_dates(df)
    assert min_date is None
    assert max_date is None
