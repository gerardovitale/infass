import logging
import os

from extractor import ingest_data

# LOGGING
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

if __name__ == "__main__":
    bucket_uri = os.getenv("INGESTION_MERC_PATH", "gs://infass/merc")
    if not bucket_uri:
        logging.error(f"An invalid Bucket URI was provided: {bucket_uri}")
        raise Exception("Bucket URI must be provided!")
    ingest_data(bucket_uri)
