from unittest.mock import MagicMock

from bigquery_reader import fetch_product_price_report


def _make_row(name, price):
    row = MagicMock()
    row.keys.return_value = ["name", "latest_price"]
    row.__iter__ = lambda self: iter(["name", "latest_price"])
    row.__getitem__ = lambda self, key: {"name": name, "latest_price": price}[key]
    return row


class TestFetchProductPriceReport:
    def test_returns_list_of_dicts(self):
        mock_client = MagicMock()
        mock_rows = [_make_row("product_a", 1.5), _make_row("product_b", 2.0)]
        mock_client.query.return_value.result.return_value = mock_rows

        result = fetch_product_price_report(mock_client, "my-project", "my-dataset")

        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["name"] == "product_a"
        assert result[1]["latest_price"] == 2.0

    def test_builds_correct_query(self):
        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = []

        fetch_product_price_report(mock_client, "proj", "ds")

        query_arg = mock_client.query.call_args[0][0]
        assert "proj.ds.product_price_report_top_ytd" in query_arg

    def test_returns_empty_list_when_no_rows(self):
        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = []

        result = fetch_product_price_report(mock_client, "proj", "ds")

        assert result == []
