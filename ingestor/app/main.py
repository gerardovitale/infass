import logging
import os
import sys

from data_builder import build_data_gen
from extractor import get_page_sources
from writer import write_pandas_to_bucket_as_csv

# LOGGING
logging.basicConfig(
    # format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    format="%(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


def ingest_data(destination_path: str) -> None:
    logging.info("Starting data ingestion")
    is_test_mode = False
    if os.getenv("TEST_MODE"):
        logging.info(f"Running test mode with: TEST_MODE = {os.getenv('TEST_MODE')}")
        is_test_mode = True
    sources = get_page_sources(is_test_mode)
    data_gen = build_data_gen(sources)
    write_pandas_to_bucket_as_csv(data_gen, destination_path)
    logging.info("Successfully ingested data}")


if __name__ == "__main__":
    bucket_uri = os.getenv("INGESTION_MERC_PATH", "gs://infass/merc")
    if not bucket_uri:
        logging.error(f"A Bucket URI must be provided: {bucket_uri}")
        sys.exit(1)

    logging.info(f"Starting data ingestion with bucket uri: {bucket_uri}")
    ingest_data(bucket_uri)
