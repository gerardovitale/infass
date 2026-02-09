import logging
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587


def _text_to_html(text: str) -> str:
    html = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    html = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", html)
    html = html.replace("\n", "<br>\n")
    return f"<html><body>{html}</body></html>"


def send_newsletter(
    sender_address: str,
    sender_password: str,
    recipients: list,
    subject: str,
    body_text: str,
) -> None:
    logger.info("Sending newsletter to %d recipients", len(recipients))

    msg = MIMEMultipart("alternative")
    msg["From"] = sender_address
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject

    msg.attach(MIMEText(body_text, "plain"))
    msg.attach(MIMEText(_text_to_html(body_text), "html"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(sender_address, sender_password)
        server.sendmail(sender_address, recipients, msg.as_string())

    logger.info("Newsletter sent successfully")
