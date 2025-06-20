import logging

import pandas as pd
from google.cloud import bigquery
from sink import Sink
from sink import Transaction

logger = logging.getLogger(__name__)


class BigQuerySink(Sink):
    def __init__(self, project_id: str, dataset_id: str, table: str, client: bigquery.Client):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table = table
        self.client = client

    def fetch_data(self, last_transaction: Transaction = None) -> pd.DataFrame:
        logger.info(f"Fetching data from BigQuery table: {self.project_id}.{self.dataset_id}.{self.table}")
        query = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.{self.table}`"
        if last_transaction:
            query += f" WHERE date >= '{last_transaction.max_date}'"
        df = self.client.query(query).to_dataframe()
        logger.info(f"Fetched {len(df)} rows from BigQuery")
        logger.info(f"DataFrame size: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        return df
