import os
import sqlite3
import tempfile
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pandas as pd
import pytest
from google.cloud import bigquery
from pandas.testing import assert_frame_equal
from sinks import BigQuerySink
from sinks import SQLiteSink
from sinks import Transaction


@pytest.fixture
def sample_df():
    return pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})


# --------------------------
# Test: BigQuerySink
# --------------------------
def test_bigquery_sink_fetch_data(sample_df):
    mock_client = Mock(spec=bigquery.Client)
    test_params = {
        "project_id": "test_project_id",
        "dataset_id": "test_dataset_id",
        "table": "test_table",
        "client": mock_client,
        "is_incremental": False,
    }

    mock_query_job = MagicMock()
    mock_query_job.to_dataframe.return_value = sample_df
    mock_client.query.return_value = mock_query_job

    sink = BigQuerySink(**test_params)
    df = sink.fetch_data()

    mock_client.query.assert_called_once()
    assert_frame_equal(df, sample_df)


# --------------------------
# Test: SQLiteSink
# --------------------------
@pytest.fixture
def mock_sqlite_connect():
    with patch("app.sinks.sqlite3.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        yield mock_connect, mock_conn


def test_sqlite_sink_write_data_when_index_columns_is_none(tmp_path, mock_sqlite_connect, sample_df):
    test_params = {
        "db_path": str(tmp_path / "test.db"),
        "table": "test_table",
    }
    mock_connect, mock_conn = mock_sqlite_connect

    sink = SQLiteSink(**test_params)

    with patch("pandas.DataFrame.to_sql") as mock_to_sql:
        sink.write_data(sample_df)
        mock_connect.assert_called_once_with(test_params["db_path"])
        mock_to_sql.assert_called_once_with(test_params["table"], mock_conn, if_exists="replace", index=False)
        mock_conn.close.assert_called_once()


def test_sqlite_sink_write_data_when_index_columns_are_specified(tmp_path, mock_sqlite_connect, sample_df):
    test_params = {
        "db_path": str(tmp_path / "test.db"),
        "table": "test_table",
        "index_columns": ["a", "b"],
    }
    mock_connect, mock_conn = mock_sqlite_connect

    sink = SQLiteSink(**test_params)

    with patch("pandas.DataFrame.to_sql") as mock_to_sql:
        sink.write_data(sample_df)
        mock_connect.assert_called_once_with(test_params["db_path"])
        mock_to_sql.assert_called_once_with(
            test_params["table"], mock_conn, if_exists="replace", index=True, index_label=test_params["index_columns"]
        )
        mock_conn.close.assert_called_once()


# --------------------------
# Test: SQLiteSink Reversed ETL transactions
# --------------------------
@pytest.fixture
def sqlite_with_retl_transactions():
    fd, db_path = tempfile.mkstemp()
    os.close(fd)
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS retl_transactions
            (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data_source_table TEXT,
                destination_table REAL,
                occurred_at TEXT,
                min_date TEXT,
                max_date TEXT
            );
            """
        )
        records = [
            # (txn.data_source_table, txn.destination_table, txn.occurred_at, txn.min_date, txn.max_date)
            ("test_data_source_table", "test_destination_table", "2025-06-07T09:00:00", "2025-01-01", "2025-06-06"),
            ("test_data_source_table", "test_destination_table", "2025-06-14T07:00:00", "2025-06-07", "2025-06-14"),
        ]
        cur.executemany(
            "INSERT INTO retl_transactions "
            "(data_source_table, destination_table, occurred_at, min_date, max_date) "
            "VALUES (?, ?, ?, ?, ?);",
            records,
        )
        conn.commit()
        yield db_path, cur
        conn.close()
    finally:
        os.remove(db_path)


def test_get_last_transaction(sqlite_with_retl_transactions):
    db_path, _ = sqlite_with_retl_transactions
    test_params = {
        "db_path": db_path,
        "table": "test_destination_table",
    }
    expected = Transaction(
        data_source_table="test_data_source_table",
        destination_table="test_destination_table",
        occurred_at="2025-06-14T07:00:00",
        min_date="2025-06-07",
        max_date="2025-06-14",
    )
    actual = SQLiteSink(**test_params).get_last_transaction_if_exist()
    assert actual == expected


@pytest.fixture
def sqlite_with_no_retl_transactions():
    fd, db_path = tempfile.mkstemp()
    os.close(fd)
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS retl_transactions
            (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data_source_table TEXT,
                destination_table REAL,
                occurred_at TEXT,
                min_date TEXT,
                max_date TEXT
            );
            """
        )
        yield db_path, cur
        conn.close()
    finally:
        os.remove(db_path)


def test_get_last_transaction_when_there_are_no_transactions(sqlite_with_no_retl_transactions):
    db_path, _ = sqlite_with_no_retl_transactions
    test_params = {
        "db_path": db_path,
        "table": "test_table",
    }
    actual = SQLiteSink(**test_params).get_last_transaction_if_exist()
    assert actual is None


@pytest.fixture
def sqlite_with_no_retl_transactions_table():
    fd, db_path = tempfile.mkstemp()
    os.close(fd)
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        yield db_path, cur
        conn.close()
    finally:
        os.remove(db_path)


def test_get_last_transaction_when_table_does_not_exist(sqlite_with_no_retl_transactions_table):
    db_path, _ = sqlite_with_no_retl_transactions_table
    test_params = {
        "db_path": db_path,
        "table": "test_table",
    }
    actual = SQLiteSink(**test_params).get_last_transaction_if_exist()
    assert actual is None
