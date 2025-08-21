import sqlite3
import tempfile
from datetime import datetime
from unittest.mock import patch

import pytest
from app.transaction_recorder import TxnRecSQLite


@pytest.fixture
def sqlite_db_path(tmp_path):
    db_path = tmp_path / "test_db.sqlite"
    conn = sqlite3.connect(db_path)
    conn.close()
    return str(db_path)


def get_sqlite_rows(db_path: str, table_name: str):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
    return rows


# ----------------------------------------------------------------------------------------------------------------------
# Test: TxnRecSQLite init
# ----------------------------------------------------------------------------------------------------------------------


def test_txn_rec_sqlite_init_with_valid_db_path(sqlite_db_path):
    product = "merc"
    data_source = "source_table"
    destination = "destination_table"

    recorder = TxnRecSQLite(sqlite_db_path, product, data_source, destination)

    assert recorder.db_path == sqlite_db_path
    assert recorder.product == product
    assert recorder.data_source == data_source
    assert recorder.destination == destination


def test_txn_rec_sqlite_init_with_non_existing_invalid_db_path():
    invalid_path = "/tmp/nonexistent_db.sqlite"
    product = "merc"
    data_source = "source_table"
    destination = "destination_table"

    with pytest.raises(Exception):
        TxnRecSQLite(invalid_path, product, data_source, destination)


def test_txn_rec_sqlite_init_with_existing_invalid_db_path():
    with tempfile.TemporaryDirectory() as temp_dir:
        invalid_path = f"{temp_dir}/nonexistent_db.sqlite"
        product = "merc"
        data_source = "source_table"
        destination = "destination_table"

    with pytest.raises(Exception):
        TxnRecSQLite(invalid_path, product, data_source, destination)


# ----------------------------------------------------------------------------------------------------------------------
# Test: TxnRecSQLite record
# ----------------------------------------------------------------------------------------------------------------------


@patch("app.transaction_recorder.datetime")
def test_txn_rec_sqlite_record_with_valid_data(mock_datetime, sqlite_db_path):
    min_date = "2025-01-01"
    max_date = "2025-08-01"
    test_recorder = TxnRecSQLite(
        db_path=sqlite_db_path,
        product="test_product",
        data_source="test_data_source",
        destination="test_destination",
    )

    mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)

    expected_transaction = (
        1,
        "test_product",
        "test_data_source",
        "test_destination",
        datetime(2025, 1, 1, 12, 0, 0).isoformat(timespec="seconds"),
        min_date,
        max_date,
    )

    test_recorder.record(min_date, max_date)

    rows = get_sqlite_rows(sqlite_db_path, test_recorder.table_name)
    assert len(rows) == 1
    assert rows[0] == expected_transaction


@patch("app.transaction_recorder.datetime")
def test_txn_rec_sqlite_record_with_valid_data_when_min_max_dates_are_none(mock_datetime, sqlite_db_path):
    min_date = None
    max_date = None
    test_recorder = TxnRecSQLite(
        db_path=sqlite_db_path,
        product="test_product",
        data_source="test_data_source",
        destination="test_destination",
    )

    mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)

    expected_transaction = (
        1,
        "test_product",
        "test_data_source",
        "test_destination",
        datetime(2025, 1, 1, 12, 0, 0).isoformat(timespec="seconds"),
        min_date,
        max_date,
    )

    test_recorder.record(min_date, max_date)

    rows = get_sqlite_rows(sqlite_db_path, test_recorder.table_name)
    assert len(rows) == 1
    assert rows[0] == expected_transaction
