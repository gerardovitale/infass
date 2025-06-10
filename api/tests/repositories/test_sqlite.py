from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from repositories import SQLiteProductRepository


@pytest.fixture
def mock_sqlite_connect():
    with patch("repositories.sqlite3.connect") as mock_connect:
        yield mock_connect


def test_search_products_returns_expected_results(mock_sqlite_connect):
    # Arrange
    test_db_path = "fake_path.db"
    test_search_term = "apple"
    expected_rows = [
        ("1", "Apple Juice", "1L", "Beverages", "Juices", 2.99, "http://img"),
        ("2", "Green Apple", "500g", "Fruits", "Apples", 1.99, "http://img2"),
    ]

    expected_dicts = [
        {
            "id": "1",
            "name": "Apple Juice",
            "size": "1L",
            "categories": "Beverages",
            "subcategories": "Juices",
            "price": 2.99,
            "image_url": "http://img",
        },
        {
            "id": "2",
            "name": "Green Apple",
            "size": "500g",
            "categories": "Fruits",
            "subcategories": "Apples",
            "price": 1.99,
            "image_url": "http://img2",
        },
    ]

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_sqlite_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.execute.return_value.fetchall.return_value = expected_rows
    # Simulate cursor.description as a list of tuples with column names
    mock_cursor.description = [
        ("id",),
        ("name",),
        ("size",),
        ("categories",),
        ("subcategories",),
        ("price",),
        ("image_url",),
    ]

    repo = SQLiteProductRepository(test_db_path)

    # Act
    results = repo.search_products(test_search_term)

    # Assert
    assert results == expected_dicts
    mock_sqlite_connect.assert_called_once_with(test_db_path)
    mock_conn.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once()
    mock_conn.close.assert_called_once()
