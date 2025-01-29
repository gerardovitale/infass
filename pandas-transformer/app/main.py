import logging
import os
import sys

from bigquery_writer import write_to_bigquery
from bucket_reader import read_csv_as_pd_df
from transformer import transformer

# LOGGING
logging.basicConfig(
    # format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    format="%(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


def run_data_transformation(bucket_data_source: str, bigquery_destination_table: str, transformer_limit) -> None:
    logging.info("Starting data transformation")
    limit = None
    if transformer_limit:
        logging.info(f"Running pandas-transformer job with: TRANSFORMER_LIMIT = {transformer_limit}")
        limit = int(transformer_limit)

    raw_data = read_csv_as_pd_df(bucket_data_source, limit)
    data = transformer(raw_data)
    write_to_bigquery(data, *bigquery_destination_table.split("."))
    logging.info(f"Successfully transformed data")


if __name__ == "__main__":
    data_source = os.getenv("DATA_SOURCE", "infass")
    destination = os.getenv("DESTINATION", "inflation-assistant.infass.merc")
    transformer_limit = os.getenv("TRANSFORMER_LIMIT")

    logging.error("This is an error message")
    raise Exception("Simulated Failure")

    if data_source and destination:
        logging.info(f"Starting data transformation with data_source and destination: {data_source}, {destination}")
        run_data_transformation(data_source, destination, transformer_limit)

    else:
        logging.error("Please provide data_source and destination")
        logging.error(f"Got data_source, destination: {data_source}, {destination}")
        sys.exit(1)
