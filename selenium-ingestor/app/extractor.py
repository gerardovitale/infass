from __future__ import annotations

import logging
import time
from datetime import datetime
from itertools import chain
from typing import Any, Dict, List

import pandas as pd
from bs4 import BeautifulSoup
from google.cloud import storage
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.expected_conditions import presence_of_element_located
from selenium.webdriver.support.ui import WebDriverWait

POSTAL_CODE = "28050"
BASE_URL = "https://tienda.mercadona.es"
WAIT_CONTENT_TIME_SLEEP = 3
COOKIES_BUTTON_XPATH = "//button[contains(text(), 'Aceptar')]"
POSTAL_CODE_INPUT_BOX_SELECTOR = "[data-testid='postal-code-checker-input']"
CATEGORY_SELECTOR = ".category-menu__item button span label"
CATEGORY_BUTTON_SELECTOR = "a.menu-item.subhead1-sb[href='/categories']"
CATEGORY_BUTTON_SELECTOR_TEMPLATE = "//label[contains(text(), '{0}')]"
SUBCATEGORY_SELECTOR = "ul li.category-item button"
SUBCATEGORY_BUTTON_SELECTOR_TEMPLATE = "//button[contains(text(), '{0}')]"

logger = logging.getLogger(__name__)


def ingest_data(destination_path: str) -> None:
    logger.info("Starting data ingestion")
    sources = get_page_sources()
    df = build_df(sources)
    df["date"] = datetime.now().date().isoformat()
    write_pandas_to_bucket_as_csv(df, destination_path)
    logger.info(f"Successfully ingested {df.shape[0]} rows of data")


def write_pandas_to_bucket_as_csv(df: pd.DataFrame, bucket_uri: str) -> None:
    logger.info(f"Writing raw pandas data as CSV to {bucket_uri}")
    storage_client = storage.Client()
    logger.info("Getting bucket")
    bucket = storage_client.get_bucket(bucket_uri)
    blob = bucket.blob(f"{datetime.now().date().isoformat()}.csv")
    blob.upload_from_string(df.to_csv(index=False), "text/csv")


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


def accept_cookies(driver: webdriver.Chrome):
    logger.info("Accepting cookies")
    accept_cookies_button = driver.find_element(By.XPATH, COOKIES_BUTTON_XPATH)
    accept_cookies_button.click()


def enter_postal_code(driver: webdriver.Chrome):
    logger.info(f"Entering postal code: {POSTAL_CODE}")
    wait = WebDriverWait(driver, 10)
    postal_code_input = wait.until(presence_of_element_located((By.CSS_SELECTOR, POSTAL_CODE_INPUT_BOX_SELECTOR)))
    postal_code_input.send_keys(POSTAL_CODE)
    postal_code_input.send_keys(Keys.ENTER)
    logger.info("Waiting for main content to load after entering postal code")
    time.sleep(WAIT_CONTENT_TIME_SLEEP)


def navigate_to_categories(driver: webdriver.Chrome):
    logger.info("Clicking the Categories button to view categories")
    categories_button = driver.find_element(By.CSS_SELECTOR, CATEGORY_BUTTON_SELECTOR)
    categories_button.click()
    time.sleep(WAIT_CONTENT_TIME_SLEEP)


def get_main_categories(driver: webdriver.Chrome) -> List[str]:
    logger.info("Collecting main category names")
    main_categories = driver.find_elements(By.CSS_SELECTOR, CATEGORY_SELECTOR)
    return [category.text for category in main_categories]


def get_subcategories(driver: webdriver.Chrome) -> List[str]:
    logger.info("Collecting subcategory names")
    subcategories = driver.find_elements(By.CSS_SELECTOR, SUBCATEGORY_SELECTOR)
    return [subcat.text for subcat in subcategories]


def extract_page_source_for_subcategory(
    driver: webdriver.Chrome, category_name: str, subcategory_name: str, click: bool = True
) -> str:
    if click:
        logger.info(f"Clicking subcategory: {subcategory_name}")
        subcategory_button = driver.find_element(
            By.XPATH, SUBCATEGORY_BUTTON_SELECTOR_TEMPLATE.format(subcategory_name)
        )
        subcategory_button.click()

    logger.info(f"Extracting page source for category '{category_name}', subcategory '{subcategory_name}'")
    time.sleep(WAIT_CONTENT_TIME_SLEEP)
    return driver.page_source


def get_page_sources() -> Dict[str, str]:
    logger.info("Getting page content")
    driver = initialize_driver()

    try:
        driver.get(BASE_URL)
        time.sleep(WAIT_CONTENT_TIME_SLEEP)

        accept_cookies(driver)
        enter_postal_code(driver)
        navigate_to_categories(driver)

        page_sources = {}
        category_names = get_main_categories(driver)

        # Iterate over each main category
        for category_name in category_names:
            logger.info(f"Clicking category: {category_name}")
            category_button = driver.find_element(By.XPATH, CATEGORY_BUTTON_SELECTOR_TEMPLATE.format(category_name))
            category_button.click()
            time.sleep(WAIT_CONTENT_TIME_SLEEP)

            # Collect subcategory names for the current category
            subcategory_names = get_subcategories(driver)

            # Get page source for the first subcategory (already loaded)
            if subcategory_names:
                first_subcategory_name = subcategory_names[0]
                page_sources[f"{category_name} > {first_subcategory_name}"] = extract_page_source_for_subcategory(
                    driver, category_name, first_subcategory_name, click=False
                )

                # For remaining subcategories, click and get page source
                for subcategory_name in subcategory_names[1:]:
                    page_sources[f"{category_name} > {subcategory_name}"] = extract_page_source_for_subcategory(
                        driver, category_name, subcategory_name
                    )

        return page_sources

    finally:
        driver.quit()


def extract_product_data(page_source: str, category: str) -> List[Dict[str, Any]]:
    logger.info(f"Extracting product data for category: {category}")
    if not page_source:
        return []

    soup = BeautifulSoup(page_source, "html.parser")
    products = soup.find_all("div", {"data-testid": "product-cell"})
    return [
        {
            "name": product.find("h4", {"data-testid": "product-cell-name"}).text.strip(),
            "original_price": (prices[0].text.strip() if prices else None),
            "discount_price": (prices[1].text.strip() if len(prices) > 1 else None),
            "size": product.find("div", class_="product-format").text.strip(),
            "category": category,
        }
        for product in products
        if (prices := product.find_all("p", {"data-testid": "product-price"}))
    ]


def build_df(page_sources: Dict[str, str]) -> pd.DataFrame:
    logger.info(f"Building dataframe from {len(page_sources)} page_sources")
    return pd.DataFrame(
        chain.from_iterable(
            extract_product_data(page_source, category) for category, page_source in page_sources.items()
        )
    )
