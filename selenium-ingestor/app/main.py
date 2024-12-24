import logging
import os

from data_builder import build_data_gen
from extractor import get_page_sources
from writer import write_pandas_to_bucket_as_csv

# LOGGING
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


def ingest_data(destination_path: str) -> None:
    logging.info("Starting data ingestion")
    sources = get_page_sources()
    data_gen = build_data_gen(sources)
    write_pandas_to_bucket_as_csv(data_gen, destination_path)
    logging.info(f"Successfully ingested data")


if __name__ == "__main__":
    bucket_uri = os.getenv("INGESTION_MERC_PATH", "gs://infass/merc")
    if not bucket_uri:
        logging.error(f"An invalid Bucket URI was provided: {bucket_uri}")
        raise Exception("Bucket URI must be provided!")
    ingest_data(bucket_uri)
