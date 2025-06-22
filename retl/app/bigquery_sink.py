import logging

import pandas as pd
from google.cloud import bigquery
from sink import Sink
from sink import Transaction

logger = logging.getLogger(__name__)


class BigQuerySink(Sink):
    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        table: str,
        client: bigquery.Client,
        start_date: str = None,
        end_date: str = None,
    ):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table = table
        self.client = client
        self.start_date = start_date
        self.end_date = end_date

    def fetch_data(self, last_transaction: Transaction = None) -> pd.DataFrame:
        logger.info(f"Fetching data from BigQuery table: {self.project_id}.{self.dataset_id}.{self.table}")
        query = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.{self.table}`"
        if last_transaction:
            query += f" WHERE date > '{last_transaction.max_date}'"
        df = self.client.query(query).to_dataframe()
        logger.info(f"Fetched {len(df)} rows from BigQuery")
        logger.info(f"DataFrame size: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        return df

    def fetch_data_by_date_range(self) -> pd.DataFrame:
        if not self.start_date or not self.end_date:
            raise ValueError("Both start_date and end_date must be provided for date range queries.")
        if self.start_date > self.end_date:
            raise ValueError(f"start_date must be earlier than end_date: {self.start_date} > {self.end_date}")
        logger.info(
            f"Fetching data from BigQuery table: {self.project_id}.{self.dataset_id}.{self.table} "
            f"for date range {self.start_date} to {self.end_date}"
        )
        query = f"""
        SELECT * FROM `{self.project_id}.{self.dataset_id}.{self.table}`
        WHERE date >= '{self.start_date}' AND date <= '{self.end_date}'
        """
        df = self.client.query(query).to_dataframe()
        logger.info(f"Fetched {len(df)} rows from BigQuery for date range {self.start_date} to {self.end_date}")
        return df
