from unittest.mock import MagicMock

import pytest
from models import ProductSearchResponse
from services import ProductService


@pytest.fixture
def mock_product_repository():
    return MagicMock()


@pytest.fixture
def product_service(mock_product_repository):
    return ProductService(product_repository=mock_product_repository)


def test_search_returns_expected_response(product_service, mock_product_repository):
    fake_products = [
        {
            "id": "123",
            "name": "coca-cola",
            "size": "330ml",
            "categories": "Beverages",
            "subcategories": "Sodas",
            "price": 1.25,
            "image_url": "https://example.com/image.jpg",
        },
    ]
    mock_product_repository.search_products.return_value = fake_products

    response = product_service.search("Cola")
    expected_query = "cola"

    assert isinstance(response, ProductSearchResponse)
    assert response.query == expected_query
    assert response.total_results == 1
    assert response.results[0].id == "123"
    assert response.results[0].name == "coca-cola"
    assert response.results[0].price == 1.25
    mock_product_repository.search_products.assert_called_once_with(expected_query)


def test_search_returns_empty_response(product_service, mock_product_repository):
    mock_product_repository.search_products.return_value = []

    response = product_service.search("nonexistent")

    assert isinstance(response, ProductSearchResponse)
    assert response.query == "nonexistent"
    assert response.total_results == 0
    assert response.results == []
    mock_product_repository.search_products.assert_called_once_with("nonexistent")


def test_return_bad_request_when_search_term_empty(product_service, mock_product_repository):
    mock_product_repository.search_products.side_effect = ValueError("Search term cannot be empty")

    with pytest.raises(ValueError, match="Search term cannot be empty"):
        product_service.search("")
