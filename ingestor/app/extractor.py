from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any
from typing import Dict
from typing import Generator
from typing import List

from bs4 import BeautifulSoup
from gcs_client import GCSClientSingleton
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.expected_conditions import presence_of_element_located
from selenium.webdriver.support.ui import WebDriverWait

POSTAL_CODE = "28050"
BASE_URL = "https://tienda.mercadona.es"
WAIT_CONTENT_TIME_SLEEP = 2
COOKIES_BUTTON_XPATH = "//button[contains(text(), 'Aceptar')]"
POSTAL_CODE_INPUT_BOX_SELECTOR = "[data-testid='postal-code-checker-input']"
CATEGORY_SELECTOR = ".category-menu__item button span label"
CATEGORY_BUTTON_SELECTOR = "a.menu-item.subhead1-sb[href='/categories']"
CATEGORY_BUTTON_SELECTOR_TEMPLATE = "//label[contains(text(), '{0}')]"
SUBCATEGORY_SELECTOR = "ul li.category-item button"
SUBCATEGORY_BUTTON_SELECTOR_TEMPLATE = "//button[contains(text(), '{0}')]"

logger = logging.getLogger(__name__)


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


def upload_to_gcs(local_file_path: str, bucket_name_name: str, destination_blob_name: str):
    try:
        storage_client = GCSClientSingleton.get_client()
        bucket_name = storage_client.bucket_name(bucket_name_name)
        blob = bucket_name.blob(destination_blob_name)
        blob.upload_from_filename(local_file_path)
        logger.info(f"Screenshot uploaded to gs://{bucket_name_name}/{destination_blob_name}")
    except Exception as e:
        logger.error(f"Failed to upload screenshot to GCS: {e}")


def save_screenshot(driver: webdriver.Chrome, filename: str, bucket_name: str, bucket_name_prefix: str = "screenshots"):
    try:
        driver.save_screenshot(filename)
        logger.error(f"Screenshot saved as {filename}")
        if bucket_name:
            timestamp = datetime.now(datetime.timezone.utc).isoformat()
            gcs_dest = f"{bucket_name_prefix}/{filename.replace('.png', '')}_{timestamp}.png"
            upload_to_gcs(filename, bucket_name, gcs_dest)
    except Exception as e:
        logger.error(f"Failed to save screenshot: {e}")
        raise


def navigate_to_categories(driver: webdriver.Chrome, bucket_name: str):
    logger.info("Clicking the Categories button to view categories")
    try:
        categories_button = driver.find_element(By.CSS_SELECTOR, CATEGORY_BUTTON_SELECTOR)
        categories_button.click()
        time.sleep(WAIT_CONTENT_TIME_SLEEP)
    except NoSuchElementException:
        logger.error(
            f"Could not find categories button. URL: {driver.current_url}\n"
            f"Page source snippet: {driver.page_source[:1000]}"
        )
        save_screenshot(driver, "navigate_to_categories_error.png", bucket_name)
        raise


def get_main_categories(driver: webdriver.Chrome) -> List[str]:
    logger.info("Collecting main category names")
    main_categories = driver.find_elements(By.CSS_SELECTOR, CATEGORY_SELECTOR)
    return [category.text for category in main_categories]


def get_subcategories(driver: webdriver.Chrome) -> List[str]:
    logger.info("Collecting subcategory names")
    subcategories = driver.find_elements(By.CSS_SELECTOR, SUBCATEGORY_SELECTOR)
    return [subcat.text for subcat in subcategories]


def get_image_url(soup: BeautifulSoup) -> str | None:
    image = soup.find("img")
    if not image:
        return None
    src = image.get("src")
    return src.strip().split("?")[0] if src else None


def extract_product_data(page_source: str, category: str) -> Generator[Dict[str, Any], None, None] | None:
    logger.info(f"Extracting product data for category: {category}")
    if not page_source:
        return

    soup = BeautifulSoup(page_source, "html.parser")
    products = soup.find_all("div", {"data-testid": "product-cell"})
    return (
        {
            "name": product.find("h4", {"data-testid": "product-cell-name"}).text.strip(),
            "original_price": (prices[0].text.strip() if prices else None),
            "discount_price": (prices[1].text.strip() if len(prices) > 1 else None),
            "size": product.find("div", class_="product-format").text.strip(),
            "category": category,
            "image_url": get_image_url(product),
        }
        for product in products
        if (prices := product.find_all("p", {"data-testid": "product-price"}))
    )


def extract_page_source_for_subcategory(
    driver: webdriver.Chrome, category_name: str, subcategory_name: str, click: bool = True
):
    if click:
        logger.info(f"Clicking subcategory: {subcategory_name}")
        subcategory_button = driver.find_element(
            By.XPATH, SUBCATEGORY_BUTTON_SELECTOR_TEMPLATE.format(subcategory_name)
        )
        subcategory_button.click()

    logger.info(f"Extracting page source for category '{category_name}', subcategory '{subcategory_name}'")
    time.sleep(WAIT_CONTENT_TIME_SLEEP)
    return extract_product_data(driver.page_source, f"{category_name} > {subcategory_name}")


def get_page_sources(test_mode: bool, bucket_name: str = None) -> Generator[Dict[str, Any], None, None]:
    logger.info("Getting page content")
    driver = initialize_driver()

    try:
        driver.get(BASE_URL)
        time.sleep(WAIT_CONTENT_TIME_SLEEP)

        try:
            accept_cookies(driver)
            enter_postal_code(driver)
            navigate_to_categories(driver, bucket_name)
        except Exception as e:
            logger.error(f"Exception during initial navigation: {e}")
            logger.error(f"Current URL: {driver.current_url}")
            logger.error(f"Page source snippet: {driver.page_source[:1000]}")
            save_screenshot(driver, "initial_navigation_error.png", bucket_name)
            raise

        product_gen_list = []
        category_names = get_main_categories(driver)

        # Iterate over each main category
        for category_name in category_names:
            logger.info(f"Clicking category: {category_name}")
            try:
                category_button = driver.find_element(By.XPATH, CATEGORY_BUTTON_SELECTOR_TEMPLATE.format(category_name))
                category_button.click()
                time.sleep(WAIT_CONTENT_TIME_SLEEP)
            except NoSuchElementException:
                logger.error(
                    f"Could not find category button for '{category_name}'. URL: {driver.current_url}\n"
                    f"Page source snippet: {driver.page_source[:1000]}"
                )
                save_screenshot(driver, "category_navigation_error.png", bucket_name)

            # Collect subcategory names for the current category
            subcategory_names = get_subcategories(driver)

            # Get page source for the first subcategory (already loaded)
            if subcategory_names:
                product_gen_list.append(
                    extract_page_source_for_subcategory(driver, category_name, subcategory_names[0], click=False)
                )

                # For remaining subcategories, click and get page source
                for subcategory_name in subcategory_names[1:]:
                    product_gen_list.append(
                        extract_page_source_for_subcategory(driver, category_name, subcategory_name)
                    )

            if test_mode:  # Stop after the first category in test mode
                logger.info("Running in test mode 🧪: Stopping after the first category")
                break

        return product_gen_list

    finally:
        driver.quit()
