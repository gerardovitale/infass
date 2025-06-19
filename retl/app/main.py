import logging
import os
from datetime import datetime
from typing import Union

from google.cloud import bigquery
from pydantic import BaseModel
from sinks import BigQuerySink
from sinks import Sink
from sinks import SQLiteSink
from sinks import Transaction

logging.basicConfig(
    format="%(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


class TaskConfig(BaseModel):
    model_config = {"arbitrary_types_allowed": True}
    data_source: Union[Sink, BigQuerySink]
    destination: Union[Sink, SQLiteSink]


def _run_task(task: TaskConfig) -> None:
    logging.info(f"Running task: {task.data_source.table} -> {task.destination.table}")
    last_txn = None
    if task.data_source.is_incremental:
        last_txn = task.destination.get_last_transaction_if_exist()
    df = task.data_source.fetch_data(last_transaction=last_txn)
    task.destination.write_data(df)
    min_date, max_date = (df["date"].min(), df["date"].max()) if "date" in df.columns else (None, None)
    task.destination.record_transaction(
        Transaction(
            data_source_table=task.data_source.table,
            destination_table=task.destination.table,
            occurred_at=datetime.now().isoformat(),
            min_date=min_date,
            max_date=max_date,
        )
    )


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
                is_incremental=False,
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
                is_incremental=True,
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
