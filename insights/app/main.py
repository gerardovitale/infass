import logging
import os

from bigquery_reader import fetch_product_price_report
from email_sender import send_newsletter
from google.cloud import bigquery
from insight_generator import generate_newsletter

logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def main():
    project_id = os.environ["BQ_PROJECT_ID"]
    dataset_id = os.environ["BQ_DATASET_ID"]
    anthropic_api_key = os.environ["ANTHROPIC_API_KEY"]
    gmail_address = os.environ["GMAIL_ADDRESS"]
    gmail_password = os.environ["GMAIL_APP_PASSWORD"]
    recipients = [r.strip() for r in os.environ["NEWSLETTER_RECIPIENTS"].split(",")]

    logger.info("Starting insights newsletter pipeline")

    client = bigquery.Client(project=project_id)
    data = fetch_product_price_report(client, project_id, dataset_id)
    if not data:
        logger.info("No data found, skipping newsletter generation")
        return

    newsletter = generate_newsletter(data, anthropic_api_key)
    send_newsletter(
        sender_address=gmail_address,
        sender_password=gmail_password,
        recipients=recipients,
        subject="Infass - Boletin de Precios",
        body_text=newsletter,
    )

    logger.info("Insights newsletter pipeline completed successfully")


if __name__ == "__main__":
    main()
