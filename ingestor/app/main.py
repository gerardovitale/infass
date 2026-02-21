import json
import logging
import os
import sys
from datetime import datetime
from datetime import timezone

from data_builder import build_data_gen
from extractor import Extractor
from extractor.carr_extractor import CarrExtractor
from extractor.merc_extractor import MercExtractor
from timing import timed_phase
from writer import write_data


# LOGGING
class _JsonFormatter(logging.Formatter):
    _EXTRA_FIELDS = ("phase", "duration_seconds", "duration_minutes")

    def format(self, record):
        log_entry = {
            "severity": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
        }
        for field in self._EXTRA_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                log_entry[field] = value
        return json.dumps(log_entry)


def _setup_logging():
    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter())
    logging.root.handlers.clear()
    logging.root.addHandler(handler)
    logging.root.setLevel(logging.INFO)


_setup_logging()


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


@timed_phase("total")
def ingest_data(data_source_url: str, dest_bucket_uri: str) -> None:
    logging.info("🚀 Starting data ingestion")
    bucket_name, bucket_prefix = dest_bucket_uri.replace("gs://", "").split("/", maxsplit=1)

    _test_mode_env = os.getenv("TEST_MODE", "").strip().lower()
    is_test_mode = bool(_test_mode_env)
    break_early = is_test_mode and _test_mode_env != "full"

    if is_test_mode:
        logging.info(f"Running test mode: TEST_MODE={_test_mode_env!r}, break_early={break_early}")

    extractor = get_extractor(data_source_url, bucket_name, break_early, is_test_mode)
    sources = extractor.get_page_sources()
    data_gen = build_data_gen(sources)
    write_data(data_gen, bucket_name, bucket_prefix, is_test_mode)


def get_extractor(
    data_source_url: str, bucket_name: str, break_early: bool = False, is_test_mode: bool = False
) -> Extractor:
    logging.info(
        f"Creating extractor for data_source_url={data_source_url}, bucket_name={bucket_name}, "
        f"break_early={break_early}, is_test_mode={is_test_mode}"
    )
    if "mercadona" in data_source_url:
        logging.info("Using MercExtractor for Mercadona data source")
        return MercExtractor(data_source_url, bucket_name, break_early, is_test_mode)

    elif "carrefour" in data_source_url:
        logging.info("Using CarrExtractor for Carrefour data source")
        return CarrExtractor(data_source_url, bucket_name, break_early, is_test_mode)

    else:
        logging.error("Unsupported data source URL")
        raise ValueError(
            f"Unsupported data source URL: {data_source_url}. Supported sources are Mercadona and Carrefour."
        )


if __name__ == "__main__":
    bucket_uri, data_source_url = parse_args()
    logging.info(f"Starting data ingestion with bucket_uri: {bucket_uri}")
    ingest_data(data_source_url, bucket_uri)
