import logging
import os

from bigquery_writer import write_to_bigquery
from bucket_reader import read_csv_as_pd_df
from transformer import transformer

# LOGGING
logging.basicConfig(
    # format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    format="%(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


def run_data_transformation(bucket_data_source: str, destination_table: str, transformer_limit, is_local_run) -> None:
    logging.info("Starting data transformation")
    limit = None
    if transformer_limit:
        logging.info(f"Running transformer job with: TRANSFORMER_LIMIT = {transformer_limit}")
        limit = int(transformer_limit)

    raw_data = read_csv_as_pd_df(bucket_data_source, limit)
    data = transformer(raw_data)

    if is_local_run:
        logging.info("Running in local mode, skipping BigQuery write")
        if not os.path.exists("data"):
            os.makedirs("data")
        logging.info(f"Writing raw data to local file data/{bucket_data_source}.csv")
        raw_data.to_csv(f"data/{bucket_data_source}.csv", index=False)
        formatted_dest_table = destination_table.replace(".", "_")
        logging.info(f"Writing data to local file data/{formatted_dest_table}.csv")
        data.to_csv(f"data/{formatted_dest_table}.csv", index=False)
        return

    logging.info(f"Writing data to BigQuery table {destination_table}")
    write_to_bigquery(data, *destination_table.split("."))
    logging.info(f"Successfully transformed data")


if __name__ == "__main__":
    data_source = os.getenv("DATA_SOURCE")
    destination = os.getenv("DESTINATION")
    transformer_limit = os.getenv("TRANSFORMER_LIMIT")
    is_local_run = os.getenv("IS_LOCAL_RUN")

    if data_source and destination:
        logging.info(f"Starting data transformation with data_source and destination: {data_source}, {destination}")
        run_data_transformation(data_source, destination, transformer_limit, is_local_run)

    else:
        logging.error("Please provide data_source and destination")
        logging.error(f"Got data_source, destination: {data_source}, {destination}")
        raise Exception("Must provide data_source and destination!")
