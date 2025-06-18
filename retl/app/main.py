import logging
import os

from google.cloud import bigquery
from pydantic import BaseModel
from sinks import BigQuerySink
from sinks import Sink
from sinks import SQLiteSink

logging.basicConfig(
    format="%(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


class TaskConfig(BaseModel):
    model_config = {"arbitrary_types_allowed": True}
    data_source: Sink
    destination: Sink


def _run_task(task: TaskConfig) -> None:
    logging.info(f"Running task: {task.data_source.table} -> {task.destination.table}")
    df = task.data_source.fetch_data()
    task.destination.write_data(df)


def run_tasks(tasks: list[TaskConfig]) -> None:
    logging.info("Starting Reversed ETL process")
    for task in tasks:
        _run_task(task)
    logging.info("Reversed ETL process completed")


def main():
    bq_project_id = os.environ["BQ_PROJECT_ID"]
    bq_dataset_id = os.environ["BQ_DATASET_ID"]
    sqlite_db_path = os.environ["SQLITE_DB_PATH"]
    bq_client = bigquery.Client(project=bq_project_id)
    tasks = [
        TaskConfig(
            data_source=BigQuerySink(
                project_id=bq_project_id,
                dataset_id=bq_dataset_id,
                table="dbt_ref_products",
                client=bq_client,
            ),
            destination=SQLiteSink(
                db_path=sqlite_db_path,
                table="products",
                index_columns=["product_id"],
            ),
        ),
        TaskConfig(
            data_source=BigQuerySink(
                project_id=bq_project_id,
                dataset_id=bq_dataset_id,
                table="dbt_ref_product_price_details",
                client=bq_client,
            ),
            destination=SQLiteSink(
                db_path=sqlite_db_path,
                table="product_price_details",
                index_columns=["date", "product_id"],
            ),
        ),
    ]
    run_tasks(tasks)


if __name__ == "__main__":
    main()
