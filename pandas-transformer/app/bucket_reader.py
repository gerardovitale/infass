import logging
from io import StringIO

import pandas as pd
from google.cloud import storage

logger = logging.getLogger(__name__)


def _list_blobs(bucket_name):
    storage_client = storage.Client()
    return storage_client.list_blobs(bucket_name)


def _read_blobs(bucket_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    with blob.open("r") as f:
        return f.read()


def read_csv_as_pd_df(bucket_name: str, limit: int = None) -> pd.DataFrame:
    logger.info(f"Getting objects from {bucket_name}")
    df_list = []
    buckets = _list_blobs(bucket_name)
    if limit:
        logger.info(f"Setting limit to {limit}")
        buckets = sorted(list(buckets))[:limit]

    for b in buckets:
        if not b.name.endswith(".csv"):
            logger.info(f"Skipping {b.name}")
            continue

        logger.info(f"Reading {b.name}")
        string_csv_data = _read_blobs(bucket_name, b.name)
        csv_data = StringIO(string_csv_data)
        df_list.append(pd.read_csv(csv_data))

    return pd.concat(df_list).reset_index().drop(columns=["index"])
