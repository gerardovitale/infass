from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Union

from bigquery_sink import BigQuerySink
from google.cloud import bigquery
from pydantic import BaseModel
from sink import Sink
from sink import Transaction
from sqlite_sink import SQLiteSink
from utils import get_min_max_dates
from utils import timeit

logging.basicConfig(
    format="%(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


class TaskConfig(BaseModel):
    model_config = {"arbitrary_types_allowed": True}
    data_source: Union[Sink, BigQuerySink]
    destination: Union[Sink, SQLiteSink]


@timeit
def _run_task(task: TaskConfig) -> None:
    logging.info(f"Running task: {task.data_source.table} -> {task.destination.table}")
    last_txn = task.destination.last_transaction
    df = task.data_source.fetch_data(last_transaction=last_txn)
    if df.empty:
        logging.warning(f"No new data to process for {task.data_source.table}")
        return
    task.destination.write_data(df)
    min_date, max_date = get_min_max_dates(df)
    task.destination.record_transaction(
        Transaction(
            data_source_table=task.data_source.table,
            destination_table=task.destination.table,
            occurred_at=datetime.now().isoformat(),
            min_date=min_date,
            max_date=max_date,
        )
    )


@timeit
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
                table="ref_products",
                client=bq_client,
            ),
            destination=SQLiteSink(
                db_path=sqlite_db_path,
                table="products",
                is_incremental=False,
                index_columns=["id"],
                fts5_config={
                    "id_column": "id",
                    "columns": ["name", "size", "categories", "subcategories"],
                },
            ),
        ),
        TaskConfig(
            data_source=BigQuerySink(
                project_id=bq_project_id,
                dataset_id=bq_dataset_id,
                table="ref_product_price_details",
                client=bq_client,
            ),
            destination=SQLiteSink(
                db_path=sqlite_db_path,
                table="product_price_details",
                is_incremental=True,
                index_columns=["date", "id"],
            ),
        ),
    ]
    run_tasks(tasks)


if __name__ == "__main__":
    main()
