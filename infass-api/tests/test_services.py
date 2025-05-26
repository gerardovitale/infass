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
    query = "cola"
    fake_products = [
        {
            "product_id": "123",
            "name": "Coca-Cola",
            "size": "330ml",
            "category": "Beverages",
            "subcategory": "Sodas",
            "price": 1.25,
            "image_url": "https://example.com/image.jpg",
        },
    ]
    mock_product_repository.search_products.return_value = fake_products

    response = product_service.search(query)

    assert isinstance(response, ProductSearchResponse)
    assert response.query == query
    assert response.total_results == 1
    assert response.results[0].product_id == "123"
    assert response.results[0].name == "Coca-Cola"
    assert response.results[0].price == 1.25
    mock_product_repository.search_products.assert_called_once_with(query)
