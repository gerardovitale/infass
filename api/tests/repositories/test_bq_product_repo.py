from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from repositories.bq_product_repo import BigQueryProductRepository

TEST_MODULE = "repositories.bq_product_repo"


@pytest.fixture
def mock_bigquery_client():
    with patch(f"{TEST_MODULE}.Client") as mock_client_class:
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        yield mock_client


class FakeBigQueryRow:
    def __init__(self, data):
        self._data = data

    def items(self):
        return self._data.items()


def test_search_products_returns_expected_results(mock_bigquery_client):
    test_bigquery_params = {"project_id": "test_project", "dataset_id": "test_dataset"}
    test_search_params = "test_search"

    expected_results = [
        {
            "id": "test_product_id",
            "name": "test_name",
            "size": "test_size",
            "categories": "test_categories",
            "subcategories": "test_subcategories",
            "current_price": "test_price",
            "image_url": "test_image_url",
        },
    ]
    # Add total_count to mock rows (simulating window function)
    mock_rows = [FakeBigQueryRow({**row, "total_count": 1}) for row in expected_results]
    mock_bigquery_client.query.return_value.result.return_value = mock_rows

    product_repo = BigQueryProductRepository(**test_bigquery_params)
    actual_products, total_count = product_repo.search_products(test_search_params)

    assert actual_products == expected_results
    assert total_count == 1
