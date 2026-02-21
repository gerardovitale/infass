from datetime import date
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from main import app
from main import get_product_service
from models import EnrichedProduct
from models import Product
from models import ProductPriceDetails


@pytest.fixture
def client():
    """Fixture to provide a test client for the FastAPI app."""
    return TestClient(app)


# ----------------------------------------------------------------------------------------------------------------------
# Test: search_products
# ----------------------------------------------------------------------------------------------------------------------
def override_product_search_response():
    test_product_search_response = [
        Product(
            id="123",
            name="Coca-Cola",
            size="330ml",
            categories="Beverages",
            subcategories="Sodas",
            current_price=1.25,
            image_url="https://img.com",
        ),
    ]
    service = MagicMock()
    service.search.return_value = (test_product_search_response, 1)
    return service


def test_search_endpoint(client):
    app.dependency_overrides[get_product_service] = override_product_search_response
    response = client.get("/products/search", params={"search_term": "cola"})
    assert response.status_code == 200
    data = response.json()
    assert data["query"] == "cola"
    assert data["total_results"] == 1
    assert data["limit"] == 20
    assert data["offset"] == 0
    assert data["has_more"] is False
    app.dependency_overrides.clear()


def test_search_endpoint_empty(client):
    app.dependency_overrides[get_product_service] = override_product_search_response
    response = client.get("/products/search", params={"search_term": ""})
    assert response.status_code == 400
    assert response.json() == {"detail": "Search term cannot be empty"}
    app.dependency_overrides.clear()


def test_search_endpoint_with_pagination(client):
    products = [
        Product(
            id=str(i),
            name=f"Product {i}",
            size="100ml",
            categories="Cat",
            subcategories="Sub",
            current_price=1.0,
            image_url=None,
        )
        for i in range(5)
    ]
    service = MagicMock()
    service.search.return_value = (products, 25)
    app.dependency_overrides[get_product_service] = lambda: service
    response = client.get("/products/search", params={"search_term": "product", "limit": 5, "offset": 0})
    assert response.status_code == 200
    data = response.json()
    assert data["total_results"] == 25
    assert data["limit"] == 5
    assert data["offset"] == 0
    assert data["has_more"] is True
    assert len(data["results"]) == 5
    app.dependency_overrides.clear()


def test_search_endpoint_limit_validation(client):
    app.dependency_overrides[get_product_service] = override_product_search_response
    response = client.get("/products/search", params={"search_term": "cola", "limit": 200})
    assert response.status_code == 422
    response = client.get("/products/search", params={"search_term": "cola", "limit": 0})
    assert response.status_code == 422
    response = client.get("/products/search", params={"search_term": "cola", "offset": -1})
    assert response.status_code == 422
    app.dependency_overrides.clear()


# ----------------------------------------------------------------------------------------------------------------------
# Test: get_product_price_details
# ----------------------------------------------------------------------------------------------------------------------
def override_enriched_product():
    test_enriched_product = EnrichedProduct(
        id="123",
        name="Coca-Cola",
        size="330 ml",
        categories="Beverages",
        subcategories="Soft Drinks",
        current_price=0.99,
        image_url="https://img.com",
        price_details=[
            ProductPriceDetails(
                date=date(2025, 6, 12),
                price=1.25,
                sma7=None,
                sma15=None,
                sma30=None,
            ),
            ProductPriceDetails(
                date=date(2025, 6, 13),
                price=0.99,
                sma7=None,
                sma15=None,
                sma30=None,
            ),
        ],
    )
    service = MagicMock()
    service.get_enriched_product.return_value = test_enriched_product
    return service


def test_get_enriched_product_endpoint(client):
    app.dependency_overrides[get_product_service] = override_enriched_product
    expected_response = {
        "id": "123",
        "name": "Coca-Cola",
        "size": "330 ml",
        "categories": "Beverages",
        "subcategories": "Soft Drinks",
        "current_price": 0.99,
        "image_url": "https://img.com",
        "price_details": [
            {
                "date": "2025-06-12",
                "price": 1.25,
                "sma7": None,
                "sma15": None,
                "sma30": None,
            },
            {
                "date": "2025-06-13",
                "price": 0.99,
                "sma7": None,
                "sma15": None,
                "sma30": None,
            },
        ],
    }
    response = client.get("/products/123")
    assert response.status_code == 200
    assert response.json() == expected_response
    app.dependency_overrides.clear()
