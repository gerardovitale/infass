import logging
import os
import sys

from data_builder import build_data_gen
from extractor import Extractor
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
    data_source_url = sys.argv[1]
    bucket_uri = sys.argv[2]
    return bucket_uri, data_source_url


def ingest_data(data_source_url: str, dest_bucket_uri: str) -> None:
    logging.info("🚀 Starting data ingestion")
    bucket_name, bucket_prefix = dest_bucket_uri.replace("gs://", "").split("/")

    is_test_mode = False
    if os.getenv("TEST_MODE"):
        logging.info(f"Running test mode with: TEST_MODE = {os.getenv('TEST_MODE')}")
        is_test_mode = True

    extractor = get_extractor(data_source_url, bucket_name, is_test_mode)
    sources = extractor.get_page_sources()
    data_gen = build_data_gen(sources)
    write_data(data_gen, bucket_name, bucket_prefix, is_test_mode)
    logging.info("✅ Successfully ingested data")


def get_extractor(data_source_url: str, bucket_name: str, test_mode: bool) -> Extractor:
    logging.info(
        f"Creating extractor for data_source_url={data_source_url}, bucket_name={bucket_name}, test_mode={test_mode},"
    )
    if "mercadona" in data_source_url:
        logging.info("Using MercExtractor for Mercadona data source")
        return MercExtractor(data_source_url, bucket_name, test_mode)

    elif "carrefour" in data_source_url:
        logging.info("Using CarrExtractor for Carrefour data source")
        return CarrExtractor(data_source_url, bucket_name, test_mode)

    else:
        logging.error("Unsupported data source URL")
        raise ValueError(
            f"Unsupported data source URL: {data_source_url}. Supported sources are Mercadona and Carrefour."
        )


if __name__ == "__main__":
    bucket_uri, data_source_url = parse_args()
    logging.info(f"Starting data ingestion with bucket_uri: {bucket_uri}")
    ingest_data(data_source_url, bucket_uri)
