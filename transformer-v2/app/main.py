import argparse
import logging
from abc import ABC
from abc import abstractmethod

import pandas as pd
from google.cloud import bigquery
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Cases to handle:

# 1. Process last date file
# - Just read last csv file from GCS
# - Transform data
# - Append to BigQuery dataset

# 2. Process a specific unprocessed date range
# - Read unprocessed CSV files from GCS
# - Transform data
# - Append to BigQuery dataset

# 3. Reprocess entire history
# - Read all CSV files from GCS
# - Transform data
# - Overwrite BigQuery dataset

# 4. Reprocess a specific date range
# - Read all CSV files from GCS within the date range
# - Transform data
# - Overwrite/Update that date range BigQuery dataset


def main():
    logging.info("Starting transformer V2 pipeline")
    args = parse_args()
    logging.info(f"Parsed args: {vars(args)}")
    params = {
        "data_source": GCS(args.gcs_source_bucket),
        "transformer": MercTransformer(),
        "destination": BigQuery(args.bq_destination_table),
    }
    run_transformer(**params)
    logging.info("Pipeline completed successfully.")


def parse_args():
    logging.info("Parsing command-line arguments")
    parser = argparse.ArgumentParser(description="Data transformation pipeline")
    parser.add_argument("--gcs-source-bucket", help="GCS source bucket name")  # required=True,
    parser.add_argument("--bq-destination-table", help="BigQuery destination table name")  # required=True,
    return parser.parse_args()


class Sink(ABC):
    def fetch_data(self) -> pd.DataFrame:
        raise NotImplementedError()

    def write_data(self, df: pd.DataFrame) -> None:
        raise NotImplementedError()


class Transformer(ABC):
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()


def run_transformer(data_source: Sink, transformer: Transformer, destination: Sink) -> None:
    logging.info(
        "Running transformer with "
        f"data_source: {data_source.__class__.__name__}, "
        f"transformer: {transformer.__class__.__name__}, "
        f"destination: {destination.__class__.__name__}"
    )
    data = data_source.fetch_data()
    transformed_data = transformer.transform(data)
    destination.write_data(transformed_data)


class GCS(Sink):
    def __init__(self, bucket_name: str):
        logging.info(f"Initializing GCS Sink for bucket: {bucket_name}")
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()

    def fetch_data(self) -> pd.DataFrame:
        pass


class MercTransformer(Transformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Executing MercTransformer")
        return df


class BigQuery(Sink):
    def __init__(self, dataset_name: str):
        logging.info(f"Initializing BigQuery Sink for table: {dataset_name}")
        self.dataset_name = dataset_name
        self.client = bigquery.Client()

    def write_data(self, df: pd.DataFrame) -> None:
        pass


if __name__ == "__main__":
    main()
