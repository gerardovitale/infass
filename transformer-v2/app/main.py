from __future__ import annotations

import argparse
import logging

from google.cloud.bigquery import TimePartitioning
from google.cloud.bigquery import TimePartitioningType
from schemas import CARR_SCHEMA
from schemas import MERC_SCHEMA
from sinks import BigQuery
from sinks import Sink
from sinks import Storage
from transformers import CarrTransformer
from transformers import MercTransformer
from transformers import Transformer
from txn_rec import TransactionRecorder
from txn_rec import TxnRecSQLite
from utils import get_min_max_dates

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


# Cases to handle:

# 1. Process last date file
# - Just read last csv file from Storage, assuming there is one per day
# - Transform data using MercTransformer, CarrTransformer, DiaTransformer...
# - Append to BigQuery dataset (merc, carr, dia)

# For example:
# Given inputs:
# - Storage source bucket: infass-landing-zone
# - Product: merc
# - BigQuery destination table: inflation-assistant.infass.merc
# The pipeline will:
# - Read last CSV file from Storage: gs://infass-landing-zone/merc/2025-08-19.csv
# - Transform data using MercTransformer
# - Append to BigQuery dataset inflation-assistant.infass.merc

# 2. Process a specific unprocessed date range
# - Read unprocessed CSV files from Storage
# - Transform data
# - Append to BigQuery dataset

# 3. Reprocess entire history
# - Read all CSV files from Storage
# - Transform data
# - Overwrite BigQuery dataset

# 4. Reprocess a specific date range
# - Read all CSV files from Storage within the date range
# - Transform data
# - Overwrite/Update that date range BigQuery dataset


def main():
    logging.info("Starting transformer V2 pipeline")
    args = parse_args()
    product_config = get_pipeline_config()[args.product]
    logging.info(f"Parsed args: {vars(args)}")
    run_transformer(
        data_source=Storage(bucket_name=args.gcs_source_bucket, prefix=args.product),
        transformer=product_config["transformer"](),
        destination=BigQuery(table_ref=args.bq_destination_table, write_config=product_config["write_config"]),
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
    parser.add_argument("--gcs-source-bucket", type=str, required=True, help="Storage source bucket name")
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


def get_pipeline_config() -> dict:
    return {
        "merc": {
            "transformer": MercTransformer,
            "write_config": {
                "create_disposition": "CREATE_IF_NEEDED",
                "write_disposition": "WRITE_APPEND",
                "schema": MERC_SCHEMA,
                "time_partitioning": TimePartitioning(
                    type_=TimePartitioningType.DAY,
                    field="date",
                ),
                "clustering_fields": ["name", "size"],
            },
        },
        "carr": {
            "transformer": CarrTransformer,
            "write_config": {
                "create_disposition": "CREATE_IF_NEEDED",
                "write_disposition": "WRITE_APPEND",
                "schema": CARR_SCHEMA,
                "time_partitioning": TimePartitioning(
                    type_=TimePartitioningType.DAY,
                    field="date",
                ),
                "clustering_fields": ["name", "size"],
            },
        },
        "dia": {},
    }


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
    last_txn = txn_recorder.get_last_txn_if_exists()
    data = data_source.fetch_data(last_txn_date=last_txn.max_date if last_txn else None)
    if data.empty:
        logging.warning("No input data found. Skipping transform/write/record.")
        return

    transformed_data = transformer.transform(data)
    if transformed_data.empty:
        logging.warning("No rows left after transformation. Skipping write/record.")
        return

    destination.write_data(transformed_data)
    txn_recorder.record(*get_min_max_dates(transformed_data))


if __name__ == "__main__":
    main()
