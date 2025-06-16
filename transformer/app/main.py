import logging
import os

from bigquery_writer import check_destination_table
from bigquery_writer import write_to_bigquery
from bucket_reader import read_csv_as_pd_df

from transformer import transform

# LOGGING
logging.basicConfig(
    format="%(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


def run_data_transformation(
    bucket_data_source: str,
    destination_table: str,
    write_disposition: str = "WRITE_TRUNCATE",
    transformer_limit=None,
    is_local_run=None,
) -> None:
    logging.info("🚀 Starting data transformation!")
    limit = None
    if transformer_limit:
        logging.info(f"Running transformer job with: TRANSFORMER_LIMIT = {transformer_limit}")
        if transformer_limit.isdigit():
            limit = int(transformer_limit)
        else:
            logging.warning(f"Invalid TRANSFORMER_LIMIT value: {transformer_limit}. Using default None limit.")
    params = {
        "bucket_data_source": bucket_data_source,
        "destination_table": destination_table,
        "limit": limit,
    }
    if is_local_run:
        run_locally(**params)
    else:
        params["write_disposition"] = write_disposition
        run_remotely(**params)
    logging.info("✅ Data transformation completed successfully")


def run_remotely(bucket_data_source: str, destination_table: str, write_disposition: str, limit: int) -> None:
    logging.info("☁️ Running remotely ☁️")
    project_id, dataset_id, table_id = destination_table.split(".")
    logging.info(f"Checking destination table: {destination_table}")
    check_destination_table(project_id, dataset_id, table_id)
    raw_data = read_csv_as_pd_df(bucket_data_source, limit)
    data = transform(raw_data)
    logging.info(f"Writing data to BigQuery table {destination_table}")
    write_to_bigquery(data, project_id, dataset_id, table_id, write_disposition)


def run_locally(bucket_data_source: str, destination_table: str, limit: int) -> None:
    logging.info("💻 Running locally 💻")
    raw_data = read_csv_as_pd_df(bucket_data_source, limit)
    data = transform(raw_data)
    os.makedirs("data", exist_ok=True)
    logging.info(f"Writing raw data to local file data/{bucket_data_source}.csv")
    raw_data.to_csv(f"data/{bucket_data_source}.csv", index=False)
    formatted_dest_table = destination_table.replace(".", "_")
    logging.info(f"Writing transformed data to local file data/{formatted_dest_table}.csv")
    data.to_csv(f"data/{formatted_dest_table}.csv", index=False)


if __name__ == "__main__":
    data_source = os.getenv("DATA_SOURCE")
    destination = os.getenv("DESTINATION")
    read_limit = os.getenv("LIMIT")  # -1 to take the last one
    write_disposition = os.getenv("WRITE_DISPOSITION")  # WRITE_TRUNCATE or WRITE_APPEND
    is_local_run = os.getenv("IS_LOCAL_RUN")

    if data_source and destination:
        logging.info(f"Starting data transformation with data_source and destination: {data_source}, {destination}")
        run_data_transformation(data_source, destination, write_disposition, read_limit, is_local_run)

    else:
        logging.error("Please provide data_source and destination")
        logging.error(f"Got data_source, destination: {data_source}, {destination}")
        raise Exception("Must provide data_source and destination!")
