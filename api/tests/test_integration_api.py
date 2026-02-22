import os
import sqlite3
import tempfile
from datetime import date
from datetime import timedelta

import pytest
from app.main import app
from app.main import get_product_service
from app.repositories.sqlite_product_repo import SQLiteProductRepository
from app.services import ProductService
from fastapi.testclient import TestClient

TODAY = date.today()
RECENT_DATE_1 = (TODAY - timedelta(days=10)).isoformat()
RECENT_DATE_2 = (TODAY - timedelta(days=9)).isoformat()
OLD_DATE = (TODAY - timedelta(days=365)).isoformat()

TEST_PRODUCTS = [
    ("1", "Apple Juice", "1L", "Beverages", "Juices", 2.99, "http://img1"),
    ("2", "Green Apple", "500g", "Fruits", "Apples", 1.99, "http://img2"),
    ("3", "Old Product", "750ml", "Beverages", "Juices", 3.49, "http://img3"),
]

TEST_PRICE_DETAILS = [
    ("1", RECENT_DATE_1, 2.89, 2.99, 2.99, 2.99),
    ("1", RECENT_DATE_2, 2.99, 2.89, 2.89, 2.89),
    ("2", RECENT_DATE_2, 1.99, None, None, None),
    ("3", OLD_DATE, 3.49, 3.49, 3.49, 3.49),
]


def setup_test_db():
    db_fd, db_path = tempfile.mkstemp(suffix=".db")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE products (
            id TEXT PRIMARY KEY,
            name TEXT,
            size TEXT,
            categories TEXT,
            subcategories TEXT,
            price REAL,
            image_url TEXT
        )
    """
    )
    cur.execute(
        """
        CREATE TABLE product_price_details (
            id TEXT,
            date TEXT,
            price REAL,
            sma7 REAL,
            sma15 REAL,
            sma30 REAL
        )
    """
    )
    cur.executemany("INSERT INTO products VALUES (?, ?, ?, ?, ?, ?, ?)", TEST_PRODUCTS)
    cur.executemany("INSERT INTO product_price_details VALUES (?, ?, ?, ?, ?, ?)", TEST_PRICE_DETAILS)
    # Create and populate FTS5 table for products
    cur.execute("DROP TABLE IF EXISTS products_fts")
    cur.execute("CREATE VIRTUAL TABLE products_fts USING fts5(id, name, size, categories, subcategories)")
    for row in TEST_PRODUCTS:
        cur.execute(
            "INSERT INTO products_fts (id, name, size, categories, subcategories) VALUES (?, ?, ?, ?, ?)",
            (row[0], row[1], row[2], row[3], row[4]),
        )
    conn.commit()
    conn.close()
    return db_fd, db_path


@pytest.fixture(scope="module")
def client():
    db_fd, db_path = setup_test_db()
    os.environ["SQLITE_DB_PATH"] = db_path

    # Override dependency to use the test db
    def override_get_product_service():
        repo = SQLiteProductRepository(db_path)
        return ProductService(repo)

    app.dependency_overrides[get_product_service] = override_get_product_service
    with TestClient(app) as c:
        yield c
    os.close(db_fd)
    os.remove(db_path)
    app.dependency_overrides.clear()


# ----------------------------------------------------------------------------------------------------------------------
# Test: search products, /products/search
# ----------------------------------------------------------------------------------------------------------------------
def test_search_products_integration(client):
    expected = {
        "query": "apple",
        "total_results": 2,
        "results": [
            {
                "id": "1",
                "name": "Apple Juice",
                "size": "1L",
                "categories": "Beverages",
                "subcategories": "Juices",
                "current_price": 2.99,
                "image_url": "http://img1",
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
        ],
        "limit": 20,
        "offset": 0,
        "has_more": False,
    }
    resp = client.get("/products/search", params={"search_term": "apple"})
    assert resp.status_code == 200
    assert resp.json() == expected


def test_search_empty_term_returns_400(client):
    resp = client.get("/products/search", params={"search_term": ""})
    assert resp.status_code == 400
    assert resp.json()["detail"] == "Search term cannot be empty"


def test_search_no_results_returns_empty(client):
    expected = {
        "query": "nonexistent",
        "total_results": 0,
        "results": [],
        "limit": 20,
        "offset": 0,
        "has_more": False,
    }
    resp = client.get("/products/search", params={"search_term": "nonexistent"})
    assert resp.status_code == 200
    assert resp.json() == expected


# ----------------------------------------------------------------------------------------------------------------------
# Test: get_enriched_product, /products/{product_id}
# ----------------------------------------------------------------------------------------------------------------------
test_cases = [
    {
        "description": "Get enriched product with valid ID returns recent price details",
        "id": 1,
        "expected_status": 200,
        "expected_response": {
            "id": "1",
            "name": "Apple Juice",
            "size": "1L",
            "categories": "Beverages",
            "subcategories": "Juices",
            "current_price": 2.99,
            "image_url": "http://img1",
            "price_details": [
                {
                    "date": RECENT_DATE_1,
                    "price": 2.89,
                    "sma7": 2.99,
                    "sma15": 2.99,
                    "sma30": 2.99,
                },
                {
                    "date": RECENT_DATE_2,
                    "price": 2.99,
                    "sma7": 2.89,
                    "sma15": 2.89,
                    "sma30": 2.89,
                },
            ],
        },
    },
    {
        "description": "Get enriched product with another valid ID",
        "id": 2,
        "expected_status": 200,
        "expected_response": {
            "id": "2",
            "name": "Green Apple",
            "size": "500g",
            "categories": "Fruits",
            "subcategories": "Apples",
            "current_price": 1.99,
            "image_url": "http://img2",
            "price_details": [
                {
                    "date": RECENT_DATE_2,
                    "price": 1.99,
                    "sma7": None,
                    "sma15": None,
                    "sma30": None,
                }
            ],
        },
    },
    {
        "description": "Get enriched product with non-existent ID",
        "id": 999,
        "expected_status": 404,
        "expected_response": {"detail": "Product not found or no price details available"},
    },
    {
        "description": "Product with only old price details returns 404 (filtered by 6-month window)",
        "id": 3,
        "expected_status": 404,
        "expected_response": {"detail": "Product not found or no price details available"},
    },
]


@pytest.mark.parametrize("test_case", test_cases, ids=[tc["description"] for tc in test_cases])
def test_get_enriched_product_integration(client, test_case):
    resp = client.get(f"/products/{test_case['id']}")
    assert resp.status_code == test_case["expected_status"]
    assert resp.json() == test_case["expected_response"]
