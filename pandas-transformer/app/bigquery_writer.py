import logging
from enum import Enum

import pandas as pd
from google.cloud import bigquery

logger = logging.getLogger(__name__)


class BigQueryWriteDis(Enum):
    WRITE_TRUNCATE = "WRITE_TRUNCATE"  # Overwrites the entire table with the new data
    WRITE_APPEND = "WRITE_APPEND"  # Appends new rows to the existing table
    WRITE_EMPTY = "WRITE_EMPTY"  # Fails the write if the table is not empty


def write_to_bigquery(df: pd.DataFrame, project_id: str, dataset_id: str, table_id: str):
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,  # Let BigQuery auto-detect schema
    )

    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Waits for the job to complete
        logger.info(f"Data successfully written to {table_ref}.")

    except Exception as e:
        logger.error(f"Error writing to BigQuery: {e}")
        raise
