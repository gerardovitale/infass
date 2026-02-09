from unittest.mock import patch

import pytest


ENV_VARS = {
    "BQ_PROJECT_ID": "test-project",
    "BQ_DATASET_ID": "test-dataset",
    "ANTHROPIC_API_KEY": "fake-key",
    "GMAIL_ADDRESS": "sender@gmail.com",
    "GMAIL_APP_PASSWORD": "password",
    "NEWSLETTER_RECIPIENTS": "r1@test.com, r2@test.com",
}


@pytest.fixture
def mock_env(monkeypatch):
    for key, value in ENV_VARS.items():
        monkeypatch.setenv(key, value)


class TestMain:
    @patch("main.send_newsletter")
    @patch("main.generate_newsletter", return_value="Newsletter text")
    @patch("main.fetch_product_price_report", return_value=[{"name": "product"}])
    @patch("main.bigquery.Client")
    def test_full_pipeline(self, mock_bq_client, mock_fetch, mock_generate, mock_send, mock_env):
        from main import main

        main()

        mock_bq_client.assert_called_once_with(project="test-project")
        mock_fetch.assert_called_once()
        mock_generate.assert_called_once_with([{"name": "product"}], "fake-key")
        mock_send.assert_called_once_with(
            sender_address="sender@gmail.com",
            sender_password="password",
            recipients=["r1@test.com", "r2@test.com"],
            subject="Infass - Boletin de Precios",
            body_text="Newsletter text",
        )

    @patch("main.send_newsletter")
    @patch("main.generate_newsletter")
    @patch("main.fetch_product_price_report", return_value=[])
    @patch("main.bigquery.Client")
    def test_skips_when_no_data(self, mock_bq_client, mock_fetch, mock_generate, mock_send, mock_env):
        from main import main

        main()

        mock_fetch.assert_called_once()
        mock_generate.assert_not_called()
        mock_send.assert_not_called()
