import logging

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud.bigquery import Client
from google.cloud.bigquery import LoadJobConfig
from google.cloud.bigquery import TimePartitioning
from google.cloud.bigquery import TimePartitioningType
from schema import BQ_MERC_SCHEMA

logger = logging.getLogger(__name__)

PARTITION_FIELD = "date"
PARTITION_TYPE = TimePartitioningType.DAY
CLUSTERING_FIELDS = ["name", "size"]


def check_destination_table(project_id: str, dataset_id: str, table_id: str):
    client = Client()
    table_id = f"{project_id}.{dataset_id}.{table_id}"
    try:
        table = client.get_table(table_id)
    except NotFound:
        raise RuntimeError(f"❌ Table '{table_id}' not found")

    partitioning_type = table.time_partitioning.type_ if table.time_partitioning else "NONE"
    partitioning_field = table.time_partitioning.field if table.time_partitioning else None
    logger.info(f"✅ Table partitioning: {partitioning_type} on field '{partitioning_field}'")
    if partitioning_type != "DAY" or partitioning_field != PARTITION_FIELD:
        raise RuntimeError(
            f"❌ Incompatible partitioning: expected DAY on 'date', got {partitioning_type} on {partitioning_field}"
        )

    actual_clustering_fields = table.clustering_fields or []
    if set(CLUSTERING_FIELDS) != set(actual_clustering_fields):
        raise RuntimeError(f"❌ Incompatible clustering: expected {CLUSTERING_FIELDS}, got {actual_clustering_fields}")


def write_to_bigquery(df: pd.DataFrame, project_id: str, dataset_id: str, table_id: str):
    client = Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    job_config = LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=BQ_MERC_SCHEMA,
        autodetect=False,
        time_partitioning=TimePartitioning(
            type_=PARTITION_TYPE,
            field=PARTITION_FIELD,
        ),
        clustering_fields=["name", "size"],
    )

    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Waits for the job to complete
        logger.info(f"Data successfully written to {table_ref}.")

    except Exception as e:
        logger.error(f"Error writing to BigQuery: {e}")
        raise
