from __future__ import annotations

import logging
import os
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

DEBUG_DIR = "data/debug"


class Extractor(metaclass=ABCMeta):
    def __init__(self, data_source_url: str, bucket_name: str, break_early: bool = False, is_test_mode: bool = False):
        self.data_source_url = data_source_url
        self.bucket_name = bucket_name
        self.break_early = break_early
        self.is_test_mode = is_test_mode

    @abstractmethod
    def get_page_sources(self) -> List[Generator[Dict[str, Any], None, None]]:
        raise NotImplementedError("Subclasses must implement this method.")

    @staticmethod
    def initialize_driver() -> webdriver.Chrome:
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--remote-debugging-port=9222")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--blink-settings=imagesEnabled=false")
        chrome_options.page_load_strategy = "eager"
        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        )
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        logger.info("Initializing driver")
        driver = webdriver.Chrome(service=Service("/usr/bin/chromedriver"), options=chrome_options)
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
        )
        return driver

    @staticmethod
    def upload_to_gcs(local_file_path: str, bucket_name: str, destination_blob_name: str):
        logger.info(
            f"Uploading screenshot to GCS: {local_file_path} to bucket: {bucket_name} as {destination_blob_name}"
        )
        try:
            storage_client = GCSClientSingleton.get_client()
            bucket = storage_client.get_bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(local_file_path)
            logger.info(f"Screenshot uploaded to gs://{bucket.name}/{destination_blob_name}")
        except Exception as e:
            logger.error(f"Failed to upload screenshot to GCS: {e}")

    def save_screenshot(
        self,
        driver: webdriver.Chrome,
        filename: str,
        bucket_name_prefix: str = "screenshots",
    ):
        try:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
            if self.is_test_mode:
                os.makedirs(DEBUG_DIR, exist_ok=True)
                filepath = os.path.join(DEBUG_DIR, f"{filename.replace('.png', '')}_{timestamp}.png")
                driver.save_screenshot(filepath)
                logger.info(f"Screenshot saved locally to {filepath}")
            else:
                driver.save_screenshot(filename)
                logger.info(f"Screenshot saved as {filename}")
                gcs_dest = f"{bucket_name_prefix}/{filename.replace('.png', '')}_{timestamp}.png"
                self.upload_to_gcs(filename, self.bucket_name, gcs_dest)
        except Exception as e:
            logger.error(f"Failed to save screenshot: {e}")
            raise

    def save_debug_html(self, driver: webdriver.Chrome, label: str, bucket_name_prefix: str = "debug_html"):
        try:
            os.makedirs(DEBUG_DIR, exist_ok=True)
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
            filename = f"{label}_{timestamp}.html"
            filepath = os.path.join(DEBUG_DIR, filename)
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            logger.info(f"Debug HTML saved to {filepath} (URL: {driver.current_url})")
            if not self.is_test_mode:
                gcs_dest = f"{bucket_name_prefix}/{filename}"
                self.upload_to_gcs(filepath, self.bucket_name, gcs_dest)
        except Exception as e:
            logger.error(f"Failed to save debug HTML: {e}")
