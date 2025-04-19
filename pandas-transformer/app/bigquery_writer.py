import logging

import pandas as pd
from google.cloud.bigquery import (
    Client,
    LoadJobConfig,
)

from schema import BQ_MERC_SCHEMA

logger = logging.getLogger(__name__)


def write_to_bigquery(df: pd.DataFrame, project_id: str, dataset_id: str, table_id: str):
    client = Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    job_config = LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=BQ_MERC_SCHEMA,
        autodetect=False,
    )

    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Waits for the job to complete
        logger.info(f"Data successfully written to {table_ref}.")

    except Exception as e:
        logger.error(f"Error writing to BigQuery: {e}")
        raise
