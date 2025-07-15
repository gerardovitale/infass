import sqlite3
import tempfile
from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest
from bigquery_sink import BigQuerySink
from main import main
from main import run_tasks
from main import TaskConfig
from sqlite_sink import SQLiteSink


@pytest.fixture
def mock_datetime_now():
    with patch("main.datetime") as mock_datetime:
        mock_datetime.now.return_value = datetime(2025, 1, 3, 0, 0, 0)
        yield mock_datetime


@pytest.fixture
def mock_bq_client():
    with patch("main.bigquery.Client") as mock_bq_client:
        yield mock_bq_client


def test_main_integration(monkeypatch, mock_datetime_now, mock_bq_client):
    # Step 1: Create a temporary SQLite database file
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp_db:
        db_path = tmp_db.name

        # Step 2: Set environment variables
        monkeypatch.setenv("BQ_PROJECT_ID", "irrelevant-for-test")
        monkeypatch.setenv("BQ_DATASET_ID", "irrelevant-for-test")
        monkeypatch.setenv("SQLITE_DB_PATH", db_path)

        # Step 3: Patch BigQuerySink.fetch_data to return test DataFrames
        products_df = pd.DataFrame(
            {
                "id": [1, 2],
                "name": ["Apple", "Banana"],
                "size": ["Medium", "Large"],
                "categories": ["Fruits", "Fruits"],
                "subcategories": ["Citrus", "Tropical"],
            }
        )
        price_details_df = pd.DataFrame(
            [
                # apple
                ("2025-01-01", 1, 0.99),
                ("2025-01-02", 1, 1.99),
                ("2025-01-03", 1, 2.99),
                # banana
                ("2025-01-01", 2, 1.45),
                ("2025-01-02", 2, 1.45),
                ("2025-01-03", 2, 1.45),
            ],
            columns=["date", "id", "price"],
        )

        with patch("main.BigQuerySink.fetch_data", side_effect=[products_df, price_details_df]):
            # Step 4: Run main()
            main()

        # Step 5: Verify the data in SQLite
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Check if 'products' table has correct data
        expected_products = [(1, "Apple", "Medium", "Fruits", "Citrus"), (2, "Banana", "Large", "Fruits", "Tropical")]
        cursor.execute("SELECT * FROM products ORDER BY id;")
        actual_products = cursor.fetchall()
        assert actual_products == expected_products

        # Check if 'product_price_details' table has correct data
        expected_product_price_details = [
            ("2025-01-01", 1, 0.99),
            ("2025-01-02", 1, 1.99),
            ("2025-01-03", 1, 2.99),
            ("2025-01-01", 2, 1.45),
            ("2025-01-02", 2, 1.45),
            ("2025-01-03", 2, 1.45),
        ]
        cursor.execute("SELECT * FROM product_price_details ORDER BY id;")
        actual_product_price_details = cursor.fetchall()
        assert actual_product_price_details == expected_product_price_details

        # Check if 'retl_transactions' table has correct data
        expected_transactions = [
            (
                1,
                "dbt_ref_products",
                "products",
                "2025-01-03T00:00:00",
                None,
                None,
            ),
            (
                2,
                "dbt_ref_product_price_details",
                "product_price_details",
                "2025-01-03T00:00:00",
                "2025-01-01",
                "2025-01-03",
            ),
        ]
        cursor.execute("SELECT * FROM retl_transactions ORDER BY id;")
        actual_transactions = cursor.fetchall()
        assert actual_transactions == expected_transactions

        # Check if FTS5 is configured correctly
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='products_fts';")
        assert cursor.fetchone() == ("products_fts",), "FTS5 table for products should exist"

        conn.close()


def test_run_tasks_incremental_scenarios(monkeypatch, mock_datetime_now):
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp_db:
        test_sqlite_db_path = tmp_db.name
        test_destination_table = "test_table"

        mock_incoming_data = [
            # Scenario 1: First write, new table, no transaction
            pd.DataFrame({"date": ["2025-01-01", "2025-01-02"], "id": [1, 2], "value": [10, 20]}),
            # Scenario 2: Second write, new records to append
            pd.DataFrame({"date": ["2025-01-03"], "id": [3], "value": [30]}),
            # Scenario 3: No new data
            pd.DataFrame(columns=["date", "id", "value"]),
        ]

        # Use the same table for all scenarios, incremental mode
        test_tasks = [
            TaskConfig(
                data_source=BigQuerySink(
                    project_id="test_project_id",
                    dataset_id="test_dataset_id",
                    table="test_bq_table",
                    client="test_client",
                ),
                destination=SQLiteSink(
                    db_path=test_sqlite_db_path,
                    table=test_destination_table,
                    is_incremental=True,
                    index_columns=["date", "id"],
                ),
            ),
        ]
        with patch("main.BigQuerySink.fetch_data", side_effect=mock_incoming_data):
            # First run: should write df1 and a transaction
            run_tasks(test_tasks)
            conn = sqlite3.connect(test_sqlite_db_path)
            cur = conn.cursor()
            cur.execute(f"SELECT * FROM {test_destination_table} ORDER BY date, id;")
            rows = cur.fetchall()
            assert rows == [("2025-01-01", 1, 10), ("2025-01-02", 2, 20)]
            cur.execute("SELECT COUNT(*) FROM retl_transactions;")
            assert cur.fetchone()[0] == 1
            conn.close()

            # Second run: should append df2 and a second transaction
            run_tasks(test_tasks)
            conn = sqlite3.connect(test_sqlite_db_path)
            cur = conn.cursor()
            cur.execute(f"SELECT * FROM {test_destination_table} ORDER BY date, id;")
            rows = cur.fetchall()
            assert rows == [("2025-01-01", 1, 10), ("2025-01-02", 2, 20), ("2025-01-03", 3, 30)]
            cur.execute("SELECT COUNT(*) FROM retl_transactions;")
            assert cur.fetchone()[0] == 2
            conn.close()

            # Third run: no new data, should not write or add transaction
            run_tasks(test_tasks)
            conn = sqlite3.connect(test_sqlite_db_path)
            cur = conn.cursor()
            cur.execute(f"SELECT * FROM {test_destination_table} ORDER BY date, id;")
            rows = cur.fetchall()
            assert rows == [("2025-01-01", 1, 10), ("2025-01-02", 2, 20), ("2025-01-03", 3, 30)]
            cur.execute("SELECT COUNT(*) FROM retl_transactions;")
            assert cur.fetchone()[0] == 2
            conn.close()
