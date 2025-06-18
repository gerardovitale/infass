import sqlite3
import tempfile
from unittest.mock import patch

import pandas as pd
from main import main


def test_main_integration(monkeypatch):
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
        prices_df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-01"],
                "product_id": [1, 2],
                "price": [0.99, 1.29],
            }
        )

        with patch("main.BigQuerySink.fetch_data", side_effect=[products_df, prices_df]):
            # Step 4: Run main()
            main()

        # Step 5: Verify the data in SQLite
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Check if 'products' table has correct data
        cursor.execute("SELECT * FROM products ORDER BY product_id")
        rows = cursor.fetchall()
        assert rows == [(1, "Apple"), (2, "Banana")]

        # Check if 'product_price_details' table has correct data
        cursor.execute("SELECT * FROM product_price_details ORDER BY product_id")
        rows = cursor.fetchall()
        assert rows == [("2024-01-01", 1, 0.99), ("2024-01-01", 2, 1.29)]

        conn.close()
