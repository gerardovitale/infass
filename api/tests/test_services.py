from unittest.mock import MagicMock

import pytest
from services import ProductService


@pytest.fixture
def mock_product_repository():
    return MagicMock()


@pytest.fixture
def product_service(mock_product_repository):
    return ProductService(product_repository=mock_product_repository)


# ----------------------------------------------------------------------------------------------------------------------
# Test: search
# ----------------------------------------------------------------------------------------------------------------------
def test_search_returns_expected_response(product_service, mock_product_repository):
    fake_products = [
        {
            "id": "123",
            "name": "coca-cola",
            "size": "330ml",
            "categories": "Beverages",
            "subcategories": "Sodas",
            "current_price": 1.25,
            "image_url": "https://example.com/image.jpg",
        },
    ]
    mock_product_repository.search_products.return_value = (fake_products, 1)

    products, total_count = product_service.search("Cola")

    assert isinstance(products, list)
    assert len(products) == 1
    assert products[0].name == "coca-cola"
    assert total_count == 1


def test_search_returns_empty_response(product_service, mock_product_repository):
    mock_product_repository.search_products.return_value = ([], 0)

    products, total_count = product_service.search("nonexistent")

    assert isinstance(products, list)
    assert len(products) == 0
    assert total_count == 0


def test_return_bad_request_when_search_term_empty(product_service, mock_product_repository):
    mock_product_repository.search_products.side_effect = ValueError("Search term cannot be empty")

    with pytest.raises(ValueError, match="Search term cannot be empty"):
        product_service.search("")


# ----------------------------------------------------------------------------------------------------------------------
# Test: get_enriched_product
# ----------------------------------------------------------------------------------------------------------------------
def test_get_enriched_product_returns_expected_response(product_service, mock_product_repository):
    fake_enriched = {
        "id": "123",
        "name": "coca-cola",
        "size": "330ml",
        "categories": "Beverages",
        "subcategories": "Sodas",
        "current_price": 1.25,
        "image_url": "https://example.com/image.jpg",
        "price_details": [
            {"date": "2025-06-12", "price": 1.25, "sma7": None, "sma15": None, "sma30": None},
        ],
    }
    mock_product_repository.get_enriched_product.return_value = fake_enriched

    result = product_service.get_enriched_product("123")

    mock_product_repository.get_enriched_product.assert_called_once_with("123", months=6)
    assert result.id == "123"
    assert result.name == "coca-cola"
    assert len(result.price_details) == 1


def test_get_enriched_product_passes_months_to_repository(product_service, mock_product_repository):
    fake_enriched = {
        "id": "456",
        "name": "water",
        "size": "1L",
        "categories": "Beverages",
        "subcategories": "Water",
        "current_price": 0.50,
        "image_url": None,
        "price_details": [
            {"date": "2025-06-12", "price": 0.50, "sma7": None, "sma15": None, "sma30": None},
        ],
    }
    mock_product_repository.get_enriched_product.return_value = fake_enriched

    result = product_service.get_enriched_product("456", months=12)

    mock_product_repository.get_enriched_product.assert_called_once_with("456", months=12)
    assert result.id == "456"
