import logging
import os
from datetime import datetime
from typing import Generator

import pandas as pd
from google.auth.transport.requests import AuthorizedSession
from google.cloud.storage import Client
from google.resumable_media.common import InvalidResponse
from google.resumable_media.requests import ResumableUpload

CHUNK_SIZE = 256 * 1024  # 256 KiB
GCP_STORAGE_RESUMABLE_UPLOAD_URL = "https://www.googleapis.com/upload/storage/v1/b/{0}/o?uploadType=resumable"

logger = logging.getLogger(__name__)


def write_data(data_gen: Generator[pd.DataFrame, None, None], destination_bucket: str, test_mode: bool):
    bucket_name, bucket_key = destination_bucket.replace("gs://", "").split("/", 1)
    if test_mode:
        write_pandas_to_local_csv(data_gen, bucket_key)
    else:
        write_pandas_to_bucket_as_csv(data_gen, bucket_name, bucket_key)


def write_pandas_to_local_csv(data_gen: Generator[pd.DataFrame, None, None], filename_prefix: str = "data") -> None:
    logger.info("Running in test mode 🧪")
    os.makedirs("data", exist_ok=True)
    output_path = f"data/{filename_prefix}_{datetime.now().date().isoformat()}.csv"
    logger.info(f"Writing data to local file: {output_path}")

    headers_written = False
    for df_chunk in data_gen:
        mode = "w" if not headers_written else "a"
        header = not headers_written

        csv_content = df_chunk.to_csv(index=False, header=header)
        with open(output_path, mode, encoding="utf-8") as f:
            f.write(csv_content)

        logger.info(
            f"Wrote chunk with{'out' if not header else ''} headers of size: {len(csv_content.encode('utf-8'))} bytes"
        )
        headers_written = True

    logger.info("Local write completed successfully.")


def write_pandas_to_bucket_as_csv(
    data_gen: Generator[pd.DataFrame, None, None], bucket_name: str, bucket_key: str
) -> None:
    logger.info(f"Writing data to bucket: {bucket_name}, key: {bucket_key}")

    storage_client = Client()

    logger.info("Getting bucket")
    bucket = storage_client.get_bucket(bucket_name)
    if not bucket:
        logger.error(f"Couldn't get the bucket: {bucket_name}")
        raise Exception

    logger.info(f"Got bucket with name: {bucket.name}")
    with StorageStreamUploader(
        storage_client, bucket.name, f"{bucket_key}/{datetime.now().date().isoformat()}.csv"
    ) as uploader:
        headers_written = False

        for df_chunk in data_gen:
            if not headers_written:
                csv_content = df_chunk.to_csv(index=False).encode("utf-8")
                logger.info(f"Uploading chunk with headers of size: {len(csv_content)} bytes")
                uploader.write(csv_content)
                headers_written = True
                continue

            csv_content = df_chunk.to_csv(index=False, header=False).encode("utf-8")
            logger.info(f"Uploading chunk without headers of size: {len(csv_content)} bytes")
            uploader.write(csv_content)

    logger.info("Upload completed successfully.")


class StorageStreamUploader(object):
    def __init__(self, client: Client, bucket_name: str, blob_name: str, chunk_size: int = CHUNK_SIZE):
        self._client = client
        self._bucket = self._client.bucket(bucket_name)
        self._blob = self._bucket.blob(blob_name)

        self._buffer = b""
        self._buffer_size = 0
        self._chunk_size = chunk_size
        self._read = 0

        self._transport = AuthorizedSession(credentials=self._client._credentials)
        self._request: ResumableUpload = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, *_):
        if exc_type is None:
            self.stop()

    def start(self):
        logger.info("Initializing upload")
        url = GCP_STORAGE_RESUMABLE_UPLOAD_URL.format(self._bucket.name)
        self._request = ResumableUpload(upload_url=url, chunk_size=self._chunk_size)
        self._request.initiate(
            transport=self._transport,
            content_type="text/csv",  # "application/octet-stream",
            stream=self,
            stream_final=False,
            metadata={"name": self._blob.name},
        )

    def stop(self):
        logger.info("Finalizing upload")
        self._request.transmit_next_chunk(self._transport)

    def write(self, data: bytes) -> int:
        data_len = len(data)
        self._buffer_size += data_len
        self._buffer += data
        del data
        while self._buffer_size >= self._chunk_size:
            try:
                logger.info(f"Transmiting next chunk of size: {self._chunk_size} bytes")
                self._request.transmit_next_chunk(self._transport)
            except InvalidResponse:
                logger.error("Invalid response received. Recovering upload.")
                self._request.recover(self._transport)
        return data_len

    def read(self, chunk_size: int) -> bytes:
        to_read = min(chunk_size, self._buffer_size)
        memview = memoryview(self._buffer)
        self._buffer = memview[to_read:].tobytes()
        self._read += to_read
        self._buffer_size -= to_read
        return memview[:to_read].tobytes()

    def tell(self) -> int:
        return self._read
