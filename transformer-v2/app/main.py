from __future__ import annotations

import argparse
import logging
from abc import ABC
from abc import abstractmethod
from typing import Optional
from typing import Tuple

import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from transaction_recorder import Transaction
from transaction_recorder import TransactionRecorder
from transaction_recorder import TxnRecSQLite

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Cases to handle:

# 1. Process last date file
# - Just read last csv file from GCS, assuming there is one per day
# - Transform data using MercTransformer, CarrTransformer, DiaTransformer...
# - Append to BigQuery dataset (merc, carr, dia)

# For example:
# Given inputs:
# - GCS source bucket: infass-landing-zone
# - Product: merc
# - BigQuery destination table: inflation-assistant.infass.merc
# The pipeline will:
# - Read last CSV file from GCS: gs://infass-landing-zone/merc/2025-08-19.csv
# - Transform data using MercTransformer
# - Append to BigQuery dataset inflation-assistant.infass.merc

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
    run_transformer(
        data_source=GCS(bucket_name=args.gcs_source_bucket, prefix=args.product),
        transformer=get_transformer(args.product),
        destination=BigQuery(dataset_name=args.bq_destination_table),
        txn_recorder=TxnRecSQLite(
            db_path="/mnt/sqlite/infass-transformer-sqlite.db",
            product=args.product,
            data_source=args.gcs_source_bucket,
            destination=args.bq_destination_table,
        ),
    )
    logging.info("Pipeline completed successfully.")


def parse_args():
    logging.info("Parsing command-line arguments")
    parser = argparse.ArgumentParser(description="Data transformation pipeline")
    parser.add_argument("--gcs-source-bucket", type=str, required=True, help="GCS source bucket name")
    parser.add_argument(
        "--product",
        type=str,
        required=True,
        choices=["merc", "carr", "dia"],
        help="Product to process",
    )
    parser.add_argument(
        "--bq-destination-table",
        type=str,
        required=True,
        help="BigQuery destination table name",
    )
    return parser.parse_args()


class Sink(ABC):
    def fetch_data(self, last_transaction: Optional[Transaction] = None) -> pd.DataFrame:
        raise NotImplementedError()

    def write_data(self, df: pd.DataFrame) -> None:
        raise NotImplementedError()


class Transformer(ABC):
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()


def get_transformer(product: str) -> Transformer:
    if product == "merc":
        return MercTransformer()
    elif product == "carr":
        raise NotImplementedError()
    elif product == "dia":
        raise NotImplementedError()
    else:
        raise ValueError(f"Unknown product: {product}")


def get_min_max_dates(d: pd.DataFrame) -> Tuple[str, str] | Tuple[None, None]:
    if not isinstance(d, pd.DataFrame):
        logging.error("Input is not a DataFrame")
        return None, None

    if "date" in d.columns:
        min_dt, max_dt = d["date"].min(), d["date"].max()
        try:
            min_date = pd.to_datetime(min_dt).date().isoformat()
            max_date = pd.to_datetime(max_dt).date().isoformat()
            return min_date, max_date
        except (ValueError, TypeError):
            logging.error(f"Invalid date values: min={min_dt}, max={max_dt}")
    return None, None


def run_transformer(
    data_source: Sink,
    transformer: Transformer,
    destination: Sink,
    txn_recorder: TransactionRecorder,
) -> None:
    logging.info(
        "Running transformer with "
        f"data_source: {data_source.__class__.__name__}, "
        f"transformer: {transformer.__class__.__name__}, "
        f"destination: {destination.__class__.__name__}, "
        f"txn_recorder: {txn_recorder.__class__.__name__} "
    )
    last_txn = txn_recorder.get_last_transaction_if_exists()
    data = data_source.fetch_data(last_transaction=last_txn)
    transformed_data = transformer.transform(data)
    destination.write_data(transformed_data)
    txn_recorder.record(*get_min_max_dates(transformed_data))


class GCS(Sink):
    def __init__(self, bucket_name: str, prefix: str):
        logging.info(f"Initializing GCS Sink for bucket: {bucket_name} with prefix: {prefix}")
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.storage_client = storage.Client()

    def fetch_data(self, last_transaction: Optional[Transaction] = None) -> pd.DataFrame:
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
