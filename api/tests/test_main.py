from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from main import app
from main import get_product_service
from models import Product
from models import ProductSearchResponse


@pytest.fixture
def client():
    """Fixture to provide a test client for the FastAPI app."""
    return TestClient(app)


def override_product_service():
    test_product_search_response = ProductSearchResponse(
        query="cola",
        total_results=1,
        results=[
            Product(
                id="123",
                name="Coca-Cola",
                size="330ml",
                categories="Beverages",
                subcategories="Sodas",
                price=1.25,
                image_url="https://img.com",
            ),
        ],
    )
    service = MagicMock()
    service.search.return_value = test_product_search_response
    return service


def test_search_endpoint(client):
    app.dependency_overrides[get_product_service] = override_product_service
    response = client.get("/products/search", params={"search_term": "cola"})
    assert response.status_code == 200
    assert response.json()["query"] == "cola"
    app.dependency_overrides.clear()


def test_search_endpoint_empty(client):
    app.dependency_overrides[get_product_service] = override_product_service
    response = client.get("/products/search", params={"search_term": ""})
    assert response.status_code == 400
    assert response.json() == {"detail": "Search term cannot be empty"}
    app.dependency_overrides.clear()
