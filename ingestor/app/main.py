import logging
import os
import sys

from data_builder import build_data_gen
from extractor.carr_extractor import CarrExtractor
from extractor.merc_extractor import MercExtractor
from writer import write_data

# LOGGING
logging.basicConfig(
    format="%(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


def parse_args():
    logging.info("Parsing command line arguments (positional)")
    if len(sys.argv) < 3:
        logging.error(
            "Insufficient arguments provided. Please provide bucket_uri and data_source_url as positional arguments."
        )
        sys.exit(1)
    bucket_uri = sys.argv[1]
    data_source_url = sys.argv[2]
    return bucket_uri, data_source_url


def ingest_data(data_source_url: str, dest_bucket_uri: str) -> None:
    logging.info("🚀 Starting data ingestion")
    bucket_name, bucket_prefix = dest_bucket_uri.replace("gs://", "").split("/")
    is_test_mode = False
    if os.getenv("TEST_MODE"):
        logging.info(f"Running test mode with: TEST_MODE = {os.getenv('TEST_MODE')}")
        is_test_mode = True
    sources = extract_page_sources(data_source_url, is_test_mode, bucket_name)
    data_gen = build_data_gen(sources)
    write_data(data_gen, bucket_name, bucket_prefix, is_test_mode)
    logging.info("✅ Successfully ingested data")


def extract_page_sources(data_source_url: str, test_mode: bool, bucket_name: str = None) -> list:
    logging.info(
        "Starting page source extraction with: "
        f"data_source_url={data_source_url}, test_mode={test_mode}, bucket_name={bucket_name}"
    )
    if "mercadona" in data_source_url:
        logging.info("Extracting data from Mercadona")
        return MercExtractor(data_source_url, test_mode, bucket_name).get_page_sources()

    elif "carrefour" in data_source_url:
        logging.info("Extracting data from Carrefour")
        return CarrExtractor(data_source_url, test_mode, bucket_name).get_page_sources()

    else:
        logging.error("Unsupported data source URL")
        raise ValueError(
            f"Unsupported data source URL: {data_source_url}. Supported sources are Mercadona and Carrefour."
        )


if __name__ == "__main__":
    bucket_uri, data_source_url = parse_args()
    logging.info(f"Starting data ingestion with bucket_uri: {bucket_uri}")
    ingest_data(data_source_url, bucket_uri)
