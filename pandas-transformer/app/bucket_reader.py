from __future__ import annotations

import logging
from io import StringIO

import pandas as pd
from google.cloud import storage

logger = logging.getLogger(__name__)


def _list_blobs(storage_client: storage.Client, bucket_name: str):
    return storage_client.list_blobs(bucket_name, prefix="merc/")


def _read_blobs(storage_client: storage.Client, bucket_name: str, blob_name: str):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    with blob.open("r") as f:
        return f.read()


def read_csv_as_pd_df(bucket_name: str, limit: int | None) -> pd.DataFrame:
    logger.info(f"Getting objects from {bucket_name} with limit: {limit}")
    df_list = []
    storage_client = storage.Client()
    blobs = _list_blobs(storage_client, bucket_name)

    blobs = sorted(blobs, key=lambda b: b.name, reverse=True)
    if limit:
        if limit == -1:
            logger.info("Reading only the most recent object")
            blobs = blobs[:1]
        else:
            logger.info(f"Setting limit to {limit}")
            blobs = blobs[:limit]

    for b in blobs:
        logger.info(f"Reading {b.name}")
        string_csv_data = _read_blobs(storage_client, bucket_name, b.name)
        csv_data = StringIO(string_csv_data)
        df_list.append(pd.read_csv(csv_data))

    logger.info(f"Read {len(df_list)} objects")
    return pd.concat(df_list).reset_index().drop(columns=["index"])
