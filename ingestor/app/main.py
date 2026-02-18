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
    bucket_name, bucket_prefix = dest_bucket_uri.replace("gs://", "").split("/", maxsplit=1)

    _test_mode_env = os.getenv("TEST_MODE", "").strip().lower()
    is_test_mode = bool(_test_mode_env)
    break_early = is_test_mode and _test_mode_env != "full"

    if is_test_mode:
        logging.info(f"Running test mode: TEST_MODE={_test_mode_env!r}, break_early={break_early}")

    extractor = get_extractor(data_source_url, bucket_name, break_early)
    sources = extractor.get_page_sources()
    data_gen = build_data_gen(sources)
    write_data(data_gen, bucket_name, bucket_prefix, is_test_mode)
    logging.info("✅ Successfully ingested data")


def get_extractor(data_source_url: str, bucket_name: str, break_early: bool = False) -> Extractor:
    logging.info(
        f"Creating extractor for data_source_url={data_source_url}, bucket_name={bucket_name}, "
        f"break_early={break_early}"
    )
    if "mercadona" in data_source_url:
        logging.info("Using MercExtractor for Mercadona data source")
        return MercExtractor(data_source_url, bucket_name, break_early)

    elif "carrefour" in data_source_url:
        logging.info("Using CarrExtractor for Carrefour data source")
        return CarrExtractor(data_source_url, bucket_name, break_early)

    else:
        logging.error("Unsupported data source URL")
        raise ValueError(
            f"Unsupported data source URL: {data_source_url}. Supported sources are Mercadona and Carrefour."
        )


if __name__ == "__main__":
    bucket_uri, data_source_url = parse_args()
    logging.info(f"Starting data ingestion with bucket_uri: {bucket_uri}")
    ingest_data(data_source_url, bucket_uri)
