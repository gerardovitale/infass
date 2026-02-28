import sqlite3
import tempfile
from datetime import datetime
from unittest.mock import patch

import pytest
from app.txn_rec import Transaction
from app.txn_rec import TxnRecSQLite


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


@patch("app.txn_rec.datetime")
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


@patch("app.txn_rec.datetime")
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


# ----------------------------------------------------------------------------------------------------------------------
# Test: TxnRecSQLite get_last_transaction_if_exists
# ----------------------------------------------------------------------------------------------------------------------


def test_txn_rec_sqlite_get_last_txn_if_exists_with_no_existing_txns(sqlite_db_path):
    test_recorder = TxnRecSQLite(
        db_path=sqlite_db_path,
        product="test_product",
        data_source="test_data_source",
        destination="test_destination",
    )

    last_txn = test_recorder.get_last_txn_if_exists()
    assert last_txn is None


@patch("app.txn_rec.datetime")
def test_txn_rec_sqlite_get_last_txn_if_exists_with_existing_txns(mock_datetime, sqlite_db_path):
    test_recorder = TxnRecSQLite(
        db_path=sqlite_db_path,
        product="test_product",
        data_source="test_data_source",
        destination="test_destination",
    )
    test_min_date = "2025-01-01"
    test_max_date = "2025-08-01"

    mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)

    expected_txn = Transaction(
        product=test_recorder.product,
        data_source=test_recorder.data_source,
        destination=test_recorder.destination,
        occurred_at=datetime(2025, 1, 1, 12, 0, 0).isoformat(timespec="seconds"),
        min_date=test_min_date,
        max_date=test_max_date,
    )

    test_recorder.record(test_min_date, test_max_date)
    last_txn = test_recorder.get_last_txn_if_exists()

    assert last_txn == expected_txn


def test_txn_rec_sqlite_get_last_txn_if_exists_after_multiple_records(sqlite_db_path):
    test_recorder = TxnRecSQLite(
        db_path=sqlite_db_path,
        product="test_product",
        data_source="test_data_source",
        destination="test_destination",
    )
    # First transaction
    test_recorder.record("2025-01-01", "2025-08-01")
    # Second transaction
    test_recorder.record("2025-02-01", "2025-09-01")
    last_txn = test_recorder.get_last_txn_if_exists()
    assert last_txn.min_date == "2025-02-01"
    assert last_txn.max_date == "2025-09-01"


def test_txn_rec_sqlite_get_last_txn_if_exists_filters_by_product(sqlite_db_path):
    merc_recorder = TxnRecSQLite(
        db_path=sqlite_db_path,
        product="merc",
        data_source="gcs_merc",
        destination="bq_merc",
    )
    carr_recorder = TxnRecSQLite(
        db_path=sqlite_db_path,
        product="carr",
        data_source="gcs_carr",
        destination="bq_carr",
    )

    merc_recorder.record("2025-01-01", "2025-06-01")
    carr_recorder.record("2025-03-01", "2025-08-01")
    merc_recorder.record("2025-02-01", "2025-09-01")

    last_carr_txn = carr_recorder.get_last_txn_if_exists()
    assert last_carr_txn is not None
    assert last_carr_txn.product == "carr"
    assert last_carr_txn.data_source == "gcs_carr"
    assert last_carr_txn.destination == "bq_carr"
    assert last_carr_txn.min_date == "2025-03-01"
    assert last_carr_txn.max_date == "2025-08-01"

    last_merc_txn = merc_recorder.get_last_txn_if_exists()
    assert last_merc_txn is not None
    assert last_merc_txn.product == "merc"
    assert last_merc_txn.min_date == "2025-02-01"
    assert last_merc_txn.max_date == "2025-09-01"


def test_txn_rec_sqlite_get_last_txn_if_exists_with_null_dates(sqlite_db_path):
    test_recorder = TxnRecSQLite(
        db_path=sqlite_db_path,
        product="test_product",
        data_source="test_data_source",
        destination="test_destination",
    )
    test_recorder.record(None, None)
    last_txn = test_recorder.get_last_txn_if_exists()
    assert last_txn.min_date is None
    assert last_txn.max_date is None
