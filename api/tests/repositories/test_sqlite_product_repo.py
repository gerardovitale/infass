import os
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from repositories.sqlite_product_repo import SQLiteProductRepository


@pytest.fixture
def mock_sqlite_connect():
    with patch("repositories.sqlite_product_repo.sqlite3.connect") as mock_connect:
        yield mock_connect


def init_sqlite_product_repo():
    # Create an instance of the repository without calling __init__
    return SQLiteProductRepository.__new__(SQLiteProductRepository)


# ----------------------------------------------------------------------------------------------------------------------
# search_products tests
# ----------------------------------------------------------------------------------------------------------------------
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
            "current_price": 2.99,
            "image_url": "http://img",
        },
        {
            "id": "2",
            "name": "Green Apple",
            "size": "500g",
            "categories": "Fruits",
            "subcategories": "Apples",
            "current_price": 1.99,
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
        ("current_price",),
        ("image_url",),
    ]

    # Patch check_db_path_exist to avoid FileNotFoundError
    with patch.object(SQLiteProductRepository, "check_db_path_exist", return_value=None):
        repo = SQLiteProductRepository(test_db_path)

        # Act
        results = repo.search_products(test_search_term)

    # Assert
    assert results == expected_dicts
    mock_sqlite_connect.assert_called_once_with(test_db_path)
    mock_conn.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once()
    mock_conn.close.assert_called_once()


# ----------------------------------------------------------------------------------------------------------------------
# check_db_path_exist tests
# ----------------------------------------------------------------------------------------------------------------------
def test_check_db_path_exist_raises_when_file_not_found(monkeypatch):
    fake_path = "/tmp/nonexistent_file_12345.db"
    # Ensure the file does not exist
    if Path(fake_path).exists():
        os.remove(fake_path)
    with pytest.raises(FileNotFoundError):
        SQLiteProductRepository.check_db_path_exist(fake_path)


def test_check_db_path_exist_passes_when_file_exists(tmp_path):
    db_file = tmp_path / "test.db"
    db_file.write_text("dummy content")
    # Should not raise
    SQLiteProductRepository.check_db_path_exist(str(db_file))


# ----------------------------------------
# get_connection tests
# ----------------------------------------
def test_get_connection_calls_sqlite_connect(monkeypatch):
    test_db_path = "some_path.db"

    repo = init_sqlite_product_repo()
    repo.db_path = test_db_path

    mock_conn = MagicMock()
    with patch("repositories.sqlite_product_repo.sqlite3.connect", return_value=mock_conn) as mock_connect:
        conn = repo.get_connection()
        mock_connect.assert_called_once_with(test_db_path)
        assert conn == mock_conn


def test_get_connection_raises_on_operational_error(monkeypatch):
    test_db_path = "bad_path.db"

    repo = init_sqlite_product_repo()
    repo.db_path = test_db_path

    with patch("repositories.sqlite_product_repo.sqlite3.connect", side_effect=sqlite3.OperationalError):
        with pytest.raises(sqlite3.OperationalError):
            repo.get_connection()


# ----------------------------------------------------------------------------------------------------------------------
# map_rows tests
# ----------------------------------------------------------------------------------------------------------------------
test_cases = [
    {
        "name": "test map_rows search products",
        "description": "Test mapping of search products data should return dict list",
        "test_rows": [
            (
                "257cfb68-0b5e-41e5-bd55-3da2384e3b09",
                "cerveza especial estrella galicia",
                "lata 330 ml",
                "bodega",
                "cerveza",
                0.89,
                "https://test-image.jpg",
            ),
            (
                "3f4c4fd1-bde5-44d8-9546-1b2d08f96d22",
                "cerveza especial estrella galicia",
                "12 botellines x 250 ml",
                "bodega",
                "cerveza",
                8.40,
                "https://test-image.jpg",
            ),
        ],
        "cursor_description": [
            ("id",),
            ("name",),
            ("size",),
            ("categories",),
            ("subcategories",),
            ("price",),
            ("image_url",),
        ],
        "expected": [
            {
                "id": "257cfb68-0b5e-41e5-bd55-3da2384e3b09",
                "name": "cerveza especial estrella galicia",
                "size": "lata 330 ml",
                "categories": "bodega",
                "subcategories": "cerveza",
                "price": 0.89,
                "image_url": "https://test-image.jpg",
            },
            {
                "id": "3f4c4fd1-bde5-44d8-9546-1b2d08f96d22",
                "name": "cerveza especial estrella galicia",
                "size": "12 botellines x 250 ml",
                "categories": "bodega",
                "subcategories": "cerveza",
                "price": 8.40,
                "image_url": "https://test-image.jpg",
            },
        ],
    },
    {
        "name": "test map_rows enriched product",
        "description": "Test mapping of enriched product data should return dict list",
        "test_rows": [
            (
                "257cfb68-0b5e-41e5-bd55-3da2384e3b09",
                "cerveza especial estrella galicia",
                "lata 330 ml",
                "bodega",
                "cerveza",
                0.89,
                "https://test-image.jpg",
                "2025-05-08",
                0.89,
                0.89,
                0.89,
                0.89,
            ),
            (
                "257cfb68-0b5e-41e5-bd55-3da2384e3b09",
                "cerveza especial estrella galicia",
                "lata 330 ml",
                "bodega",
                "cerveza",
                0.89,
                "https://test-image.jpg",
                "2025-05-09",
                0.89,
                0.89,
                0.89,
                0.89,
            ),
        ],
        "cursor_description": [
            ("id",),
            ("name",),
            ("size",),
            ("categories",),
            ("subcategories",),
            ("current_price",),
            ("image_url",),
            ("date",),
            ("price",),
            ("sma7",),
            ("sma15",),
            ("sma30",),
        ],
        "expected": [
            {
                "id": "257cfb68-0b5e-41e5-bd55-3da2384e3b09",
                "name": "cerveza especial estrella galicia",
                "size": "lata 330 ml",
                "categories": "bodega",
                "subcategories": "cerveza",
                "current_price": 0.89,
                "image_url": "https://test-image.jpg",
                "date": "2025-05-08",
                "price": 0.89,
                "sma7": 0.89,
                "sma15": 0.89,
                "sma30": 0.89,
            },
            {
                "id": "257cfb68-0b5e-41e5-bd55-3da2384e3b09",
                "name": "cerveza especial estrella galicia",
                "size": "lata 330 ml",
                "categories": "bodega",
                "subcategories": "cerveza",
                "current_price": 0.89,
                "image_url": "https://test-image.jpg",
                "date": "2025-05-09",
                "price": 0.89,
                "sma7": 0.89,
                "sma15": 0.89,
                "sma30": 0.89,
            },
        ],
    },
]


@pytest.mark.parametrize(
    "test_rows,cursor_description,expected",
    [(test_case["test_rows"], test_case["cursor_description"], test_case["expected"]) for test_case in test_cases],
)
def test_map_rows(test_rows, cursor_description, expected):
    test_cursor = MagicMock()
    test_cursor.description = cursor_description
    repo = init_sqlite_product_repo()
    actual = repo.map_rows(test_rows, test_cursor)
    assert actual == expected
