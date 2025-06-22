from __future__ import annotations

import logging
import os
from pathlib import Path

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
    data_source: str,
    destination: str,
    write_disposition: str,
    read_limit: str = None,
    is_local_run: bool = False,
) -> None:
    logging.info("🚀 Starting data transformation!")
    params = {
        "data_source": data_source,
        "destination": destination,
        "limit": parse_limit(read_limit),
    }
    if is_local_run:
        run_locally(**params)
    else:
        params.update({"write_disposition": write_disposition})
        run_remotely(**params)
    logging.info("✅ Data transformation completed successfully")


def parse_limit(read_limit: str) -> int | None:
    limit = None
    if read_limit:
        logging.info(f"Running transformer job with: LIMIT = {read_limit}")
        if str(read_limit).isdigit():
            limit = int(read_limit)
        else:
            logging.warning(f"Invalid LIMIT value: {read_limit}. Using default None limit.")
    return limit


def run_remotely(data_source: str, destination: str, write_disposition: str, limit: int = None) -> None:
    logging.info("☁️ Running remotely ☁️")
    project_id, dataset_id, table_id = destination.split(".")
    logging.info(f"Checking destination table: {destination}")
    check_destination_table(project_id, dataset_id, table_id)
    raw_data = read_csv_as_pd_df(data_source, limit)
    data = transform(raw_data)
    logging.info(f"Writing data to BigQuery table {destination}")
    write_to_bigquery(data, project_id, dataset_id, table_id, write_disposition)


def run_locally(data_source: str, destination: str, limit: int = None) -> None:
    logging.info("💻 Running locally 💻")
    raw_data = read_csv_as_pd_df(data_source, limit)
    data = transform(raw_data)
    Path("data").mkdir(exist_ok=True)
    raw_data_path = Path("data") / f"{data_source}.csv"
    logging.info(f"Writing raw data to local file {raw_data_path}")
    raw_data.to_csv(raw_data_path, index=False)
    formatted_dest_table = destination.replace(".", "_")
    transformed_path = Path("data") / f"{formatted_dest_table}.csv"
    logging.info(f"Writing transformed data to local file {transformed_path}")
    data.to_csv(transformed_path, index=False)


if __name__ == "__main__":
    config_params = {
        "data_source": os.getenv("DATA_SOURCE"),
        "destination": os.getenv("DESTINATION"),
        "read_limit": os.getenv("LIMIT"),
        "write_disposition": os.getenv("WRITE_DISPOSITION"),
        "is_local_run": os.getenv("IS_LOCAL_RUN") in ("true", "1", "yes"),
    }

    if config_params["data_source"] and config_params["destination"]:
        logging.info(
            f"Starting data transformation with data_source and destination: "
            f"{config_params['data_source']}, {config_params['destination']}"
        )
        run_data_transformation(**config_params)

    else:
        logging.error("Please provide data_source and destination")
        logging.error(f"Got data_source, destination: {config_params['data_source']}, {config_params['destination']}")
        raise Exception("Must provide data_source and destination!")
