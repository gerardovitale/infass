import logging
import os
import sys

from data_builder import build_data_gen
from extractor import get_page_sources
from writer import write_data

# LOGGING
logging.basicConfig(
    format="%(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


def ingest_data(dest_bucket_uri: str) -> None:
    logging.info("🚀 Starting data ingestion")
    bucket_name, bucket_prefix = dest_bucket_uri.replace("gs://", "").split("/")
    is_test_mode = False
    if os.getenv("TEST_MODE"):
        logging.info(f"Running test mode with: TEST_MODE = {os.getenv('TEST_MODE')}")
        is_test_mode = True
    sources = get_page_sources(is_test_mode, bucket_name)
    data_gen = build_data_gen(sources)
    write_data(data_gen, bucket_name, bucket_prefix, is_test_mode)
    logging.info("✅ Successfully ingested data")


if __name__ == "__main__":
    bucket_uri = os.getenv("INGESTION_MERC_PATH", "gs://infass-merc/merc")
    if not bucket_uri:
        logging.error(f"A Bucket URI must be provided: {bucket_uri}")
        sys.exit(1)

    logging.info(f"Starting data ingestion with bucket uri: {bucket_uri}")
    ingest_data(bucket_uri)
