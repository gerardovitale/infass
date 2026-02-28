import sqlite3

import pandas as pd
import pytest
from sink import Transaction
from sqlite_sink import SQLiteSink


@pytest.fixture
def sample_df():
    return pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})


# --------------------------
# Test: SQLiteSink write_data
# --------------------------


def test_sqlite_sink_write_data_when_index_columns_is_none(tmp_path, sample_df):
    db_path = str(tmp_path / "test.db")
    sink = SQLiteSink(db_path=db_path, table="test_table")

    sink.write_data(sample_df)

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM test_table")
        rows = cur.fetchall()
    assert len(rows) == 2
    assert rows[0] == (1, "x")
    assert rows[1] == (2, "y")


def test_sqlite_sink_write_data_when_index_columns_are_specified(tmp_path, sample_df):
    db_path = str(tmp_path / "test.db")
    sink = SQLiteSink(db_path=db_path, table="test_table", index_columns=["a", "b"])

    sink.write_data(sample_df)

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM test_table")
        rows = cur.fetchall()
    assert len(rows) == 2
    assert rows[0] == (1, "x")
    assert rows[1] == (2, "y")


# --------------------------
# Test: SQLiteSink Reversed ETL transactions
# --------------------------


@pytest.fixture
def sqlite_with_retl_transactions(tmp_path):
    db_path = str(tmp_path / "retl.db")
    sink = SQLiteSink(db_path=db_path, table="test_destination_table")
    txns = [
        Transaction(
            data_source_table="test_data_source_table",
            destination_table="test_destination_table",
            occurred_at="2025-06-07T09:00:00",
            min_date="2025-01-01",
            max_date="2025-06-06",
        ),
        Transaction(
            data_source_table="test_data_source_table",
            destination_table="test_destination_table",
            occurred_at="2025-06-14T07:00:00",
            min_date="2025-06-07",
            max_date="2025-06-14",
        ),
    ]
    for txn in txns:
        sink.record_transaction(txn)
    return db_path


def test_get_last_transaction(sqlite_with_retl_transactions):
    db_path = sqlite_with_retl_transactions
    expected = Transaction(
        data_source_table="test_data_source_table",
        destination_table="test_destination_table",
        occurred_at="2025-06-14T07:00:00",
        min_date="2025-06-07",
        max_date="2025-06-14",
    )
    actual = SQLiteSink(db_path=db_path, table="test_destination_table").get_last_transaction_if_exist()
    assert actual.model_dump(exclude={"id"}) == expected.model_dump(exclude={"id"})


def test_get_last_transaction_when_there_are_no_transactions(tmp_path):
    db_path = str(tmp_path / "empty.db")
    sink = SQLiteSink(db_path=db_path, table="test_table")
    actual = sink.get_last_transaction_if_exist()
    assert actual is None


def test_get_last_transaction_when_table_is_empty(tmp_path):
    db_path = str(tmp_path / "empty_table.db")
    sink = SQLiteSink(db_path=db_path, table="test_table")
    actual = sink.get_last_transaction_if_exist()
    assert actual is None


# --------------------------
# Test: SQLiteSink write_data incremental
# --------------------------


def test_sqlite_sink_write_data_incremental_appends_when_last_transaction_exists(tmp_path):
    db_path = str(tmp_path / "test.db")
    sink = SQLiteSink(db_path=db_path, table="test_table", is_incremental=True, index_columns=["id"])

    # First write (replace mode, no prior transaction)
    df1 = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
    sink.write_data(df1)
    sink.record_transaction(
        Transaction(
            data_source_table="src",
            destination_table="test_table",
            occurred_at="2025-01-01T00:00:00",
            min_date="2025-01-01",
            max_date="2025-01-01",
        )
    )

    # Create a new sink instance so last_transaction is fetched fresh
    sink2 = SQLiteSink(db_path=db_path, table="test_table", is_incremental=True, index_columns=["id"])
    df2 = pd.DataFrame({"id": [3], "value": [30]})
    sink2.write_data(df2)

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM test_table ORDER BY id")
        rows = cur.fetchall()
    assert len(rows) == 3
    assert rows == [(1, 10), (2, 20), (3, 30)]


def test_sqlite_sink_write_data_incremental_replaces_when_no_prior_transaction(tmp_path):
    db_path = str(tmp_path / "test.db")
    sink = SQLiteSink(db_path=db_path, table="test_table", is_incremental=True)

    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    sink.write_data(df)

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM test_table")
        rows = cur.fetchall()
    assert len(rows) == 2
    assert rows[0] == (1, "x")


# --------------------------
# Test: SQLiteSink create_or_refresh_fts5_table
# --------------------------


def test_create_or_refresh_fts5_table_creates_searchable_index(tmp_path):
    db_path = str(tmp_path / "test.db")
    sink = SQLiteSink(db_path=db_path, table="products")

    df = pd.DataFrame({"id": [1, 2], "name": ["Apple", "Banana"], "category": ["Fruit", "Fruit"]})
    sink.write_data(df)

    with sink.engine.connect() as conn:
        sink.create_or_refresh_fts5_table(conn, id_column="id", columns=["name", "category"])

    # Verify FTS5 table exists and is searchable
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='products_fts'")
        assert cur.fetchone() is not None, "FTS5 table should exist"

        cur.execute("SELECT id, name FROM products_fts WHERE products_fts MATCH 'Apple'")
        results = cur.fetchall()
        assert len(results) == 1
        assert results[0] == (1, "Apple")


def test_create_or_refresh_fts5_table_refreshes_on_second_call(tmp_path):
    db_path = str(tmp_path / "test.db")
    sink = SQLiteSink(db_path=db_path, table="products")

    df1 = pd.DataFrame({"id": [1], "name": ["Apple"]})
    sink.write_data(df1)
    with sink.engine.connect() as conn:
        sink.create_or_refresh_fts5_table(conn, id_column="id", columns=["name"])

    # Replace table data and refresh FTS5
    df2 = pd.DataFrame({"id": [2], "name": ["Banana"]})
    sink.write_data(df2)
    with sink.engine.connect() as conn:
        sink.create_or_refresh_fts5_table(conn, id_column="id", columns=["name"])

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, name FROM products_fts WHERE products_fts MATCH 'Apple'")
        assert cur.fetchall() == [], "Old data should not be in refreshed FTS5"

        cur.execute("SELECT id, name FROM products_fts WHERE products_fts MATCH 'Banana'")
        results = cur.fetchall()
        assert len(results) == 1
        assert results[0] == (2, "Banana")


# --------------------------
# Test: SQLiteSink last_transaction property caching
# --------------------------


def test_last_transaction_property_returns_none_when_not_incremental(tmp_path):
    db_path = str(tmp_path / "test.db")
    sink = SQLiteSink(db_path=db_path, table="test_table", is_incremental=False)
    sink.record_transaction(
        Transaction(
            data_source_table="src",
            destination_table="test_table",
            occurred_at="2025-01-01T00:00:00",
            min_date=None,
            max_date=None,
        )
    )
    assert sink.last_transaction is None


def test_last_transaction_property_caches_result_when_incremental(tmp_path):
    db_path = str(tmp_path / "test.db")
    sink = SQLiteSink(db_path=db_path, table="test_table", is_incremental=True)
    sink.record_transaction(
        Transaction(
            data_source_table="src",
            destination_table="test_table",
            occurred_at="2025-01-01T00:00:00",
            min_date="2025-01-01",
            max_date="2025-01-01",
        )
    )

    first_call = sink.last_transaction
    assert first_call is not None
    assert first_call.destination_table == "test_table"

    # Record another transaction — property should return cached (first) result
    sink.record_transaction(
        Transaction(
            data_source_table="src",
            destination_table="test_table",
            occurred_at="2025-02-01T00:00:00",
            min_date="2025-02-01",
            max_date="2025-02-01",
        )
    )
    second_call = sink.last_transaction
    assert second_call.occurred_at == first_call.occurred_at, "Property should return cached value"


def test_sqlite_sink_record_transaction_creates_table_and_inserts(tmp_path):
    db_path = str(tmp_path / "test.db")
    sink = SQLiteSink(db_path=db_path, table="test_table")
    txn = Transaction(
        data_source_table="src",
        destination_table="test_table",
        occurred_at="2025-06-14T07:00:00",
        min_date="2025-06-07",
        max_date="2025-06-14",
    )
    sink.record_transaction(txn)

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM retl_transactions WHERE destination_table = ?", ("test_table",))
        rows = cur.fetchall()
    assert len(rows) == 1
