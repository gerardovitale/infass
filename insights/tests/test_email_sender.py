from unittest.mock import MagicMock
from unittest.mock import patch

from email_sender import _text_to_html
from email_sender import send_newsletter


class TestTextToHtml:
    def test_converts_bold(self):
        assert "<strong>hello</strong>" in _text_to_html("**hello**")

    def test_converts_newlines(self):
        result = _text_to_html("line1\nline2")
        assert "<br>" in result

    def test_escapes_html_entities(self):
        result = _text_to_html("a < b & c > d")
        assert "&lt;" in result
        assert "&amp;" in result
        assert "&gt;" in result

    def test_wraps_in_html_tags(self):
        result = _text_to_html("text")
        assert result.startswith("<html><body>")
        assert result.endswith("</body></html>")


class TestSendNewsletter:
    @patch("email_sender.smtplib.SMTP")
    def test_sends_email_via_smtp(self, mock_smtp_cls):
        mock_server = MagicMock()
        mock_smtp_cls.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)

        send_newsletter(
            sender_address="sender@gmail.com",
            sender_password="password",
            recipients=["r1@test.com", "r2@test.com"],
            subject="Test Subject",
            body_text="Hello",
        )

        mock_smtp_cls.assert_called_once_with("smtp.gmail.com", 587)
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with("sender@gmail.com", "password")
        mock_server.sendmail.assert_called_once()
        sendmail_args = mock_server.sendmail.call_args[0]
        assert sendmail_args[0] == "sender@gmail.com"
        assert sendmail_args[1] == ["r1@test.com", "r2@test.com"]
