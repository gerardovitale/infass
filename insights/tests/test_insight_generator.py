import json
from unittest.mock import MagicMock
from unittest.mock import patch

from insight_generator import generate_newsletter
from insight_generator import SYSTEM_PROMPT


class TestGenerateNewsletter:
    def test_returns_generated_text(self):
        mock_client = MagicMock()
        mock_client.messages.create.return_value.content = [MagicMock(text="Newsletter content")]

        with patch("insight_generator.anthropic.Anthropic", return_value=mock_client):
            result = generate_newsletter([{"name": "test"}], "fake-key")

        assert result == "Newsletter content"

    def test_sends_data_as_json_in_prompt(self):
        mock_client = MagicMock()
        mock_client.messages.create.return_value.content = [MagicMock(text="ok")]
        data = [{"name": "product_a", "ytd_price_var_pct": 0.5}]

        with patch("insight_generator.anthropic.Anthropic", return_value=mock_client):
            generate_newsletter(data, "fake-key")

        call_args = mock_client.messages.create.call_args
        user_message = call_args.kwargs["messages"][0]["content"]
        assert json.dumps(data, ensure_ascii=False, indent=2) in user_message

    def test_uses_system_prompt(self):
        mock_client = MagicMock()
        mock_client.messages.create.return_value.content = [MagicMock(text="ok")]

        with patch("insight_generator.anthropic.Anthropic", return_value=mock_client):
            generate_newsletter([], "fake-key")

        call_args = mock_client.messages.create.call_args
        assert call_args.kwargs["system"] == SYSTEM_PROMPT

    def test_uses_custom_model(self):
        mock_client = MagicMock()
        mock_client.messages.create.return_value.content = [MagicMock(text="ok")]

        with patch("insight_generator.anthropic.Anthropic", return_value=mock_client):
            generate_newsletter([], "fake-key", model="claude-haiku-4-5-20251001")

        call_args = mock_client.messages.create.call_args
        assert call_args.kwargs["model"] == "claude-haiku-4-5-20251001"

    def test_passes_api_key_to_client(self):
        with patch("insight_generator.anthropic.Anthropic") as mock_cls:
            mock_cls.return_value.messages.create.return_value.content = [MagicMock(text="ok")]
            generate_newsletter([], "my-secret-key")

        mock_cls.assert_called_once_with(api_key="my-secret-key")
