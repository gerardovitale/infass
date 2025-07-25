from __future__ import annotations

import logging
from abc import ABCMeta
from abc import abstractmethod
from datetime import datetime
from datetime import timezone
from typing import Any
from typing import Dict
from typing import Generator
from typing import List

from gcs_client import GCSClientSingleton
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

logger = logging.getLogger(__name__)


class Extractor(metaclass=ABCMeta):
    def __init__(self, data_source_url: str, test_mode: bool, bucket_name: str | None = None):
        self.data_source_url = data_source_url
        self.test_mode = test_mode
        self.bucket_name = bucket_name

    @abstractmethod
    def get_page_sources(self) -> List[Generator[Dict[str, Any], None, None]]:
        raise NotImplementedError("Subclasses must implement this method.")

    @staticmethod
    def initialize_driver() -> webdriver.Chrome:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-dev-tools")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--remote-debugging-port=9222")
        logger.info("Initializing driver with webdriver-manager")
        return webdriver.Chrome(service=Service("/usr/bin/chromedriver"), options=chrome_options)

    @staticmethod
    def upload_to_gcs(local_file_path: str, bucket_name_name: str, destination_blob_name: str):
        try:
            storage_client = GCSClientSingleton.get_client()
            bucket_name = storage_client.bucket_name(bucket_name_name)
            blob = bucket_name.blob(destination_blob_name)
            blob.upload_from_filename(local_file_path)
            logger.info(f"Screenshot uploaded to gs://{bucket_name_name}/{destination_blob_name}")
        except Exception as e:
            logger.error(f"Failed to upload screenshot to GCS: {e}")

    def save_screenshot(
        self,
        driver: webdriver.Chrome,
        filename: str,
        bucket_name: str,
        bucket_name_prefix: str = "screenshots",
    ):
        try:
            driver.save_screenshot(filename)
            logger.error(f"Screenshot saved as {filename}")
            if bucket_name:
                timestamp = datetime.now(timezone.utc).isoformat()
                gcs_dest = f"{bucket_name_prefix}/{filename.replace('.png', '')}_{timestamp}.png"
                self.upload_to_gcs(filename, bucket_name, gcs_dest)
        except Exception as e:
            logger.error(f"Failed to save screenshot: {e}")
            raise
