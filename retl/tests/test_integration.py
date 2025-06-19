import sqlite3
import tempfile
from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest
from main import main


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
                "product_id": [1, 2],
                "name": ["Apple", "Banana"],
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
            columns=["date", "product_id", "price"],
        )

        with patch("main.BigQuerySink.fetch_data", side_effect=[products_df, price_details_df]):
            # Step 4: Run main()
            main()

        # Step 5: Verify the data in SQLite
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Check if 'products' table has correct data
        expected_products = [(1, "Apple"), (2, "Banana")]
        cursor.execute("SELECT * FROM products ORDER BY product_id;")
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
        cursor.execute("SELECT * FROM product_price_details ORDER BY product_id;")
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

        conn.close()
