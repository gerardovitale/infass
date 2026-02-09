import logging

from google.cloud import bigquery

logger = logging.getLogger(__name__)


def fetch_product_price_report(client: bigquery.Client, project_id: str, dataset_id: str) -> list:
    query = f"SELECT * FROM `{project_id}.{dataset_id}.product_price_report_top_ytd`"
    logger.info("Fetching product price report from BigQuery")
    rows = client.query(query).result()
    data = [dict(row) for row in rows]
    logger.info("Fetched %d rows from product_price_report_top_ytd", len(data))
    return data
