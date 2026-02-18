from __future__ import annotations

import logging
from typing import Any
from typing import Dict
from typing import Generator
from typing import List
from typing import Optional
from urllib.parse import urlparse
from urllib.parse import urlunparse

from bs4 import BeautifulSoup
from extractor import Extractor
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

logger = logging.getLogger(__name__)

SKIP_CATEGORIES = {"Mis productos", "Ofertas"}


def get_carr_image_url(product_soup: BeautifulSoup) -> Optional[str]:
    image = product_soup.find("img", class_="product-card__image")
    if not image:
        return None
    src = image.get("data-src")
    if not src or not isinstance(src, str):
        return None
    parsed = urlparse(src.strip())
    return urlunparse(parsed._replace(query="", fragment=""))


def extract_carr_product_data(page_source: str, category: str) -> Generator[Dict[str, Any], None, None]:
    logger.info(f"Extracting Carrefour product data for category: {category}")
    if not page_source:
        return

    soup = BeautifulSoup(page_source, "html.parser")
    products = soup.find_all("div", class_="product-card__parent")

    for product in products:
        title_link = product.select_one("h2.product-card__title a.product-card__title-link")
        if not title_link:
            continue
        name = title_link.text.strip()

        strikethrough = product.select_one("span.product-card__price--strikethrough")
        current_price = product.select_one("span.product-card__price--current")

        if strikethrough and current_price:
            original_price = strikethrough.text.strip()
            discount_price = current_price.text.strip()
        else:
            price_el = product.select_one("span.product-card__price")
            original_price = price_el.text.strip() if price_el else None
            discount_price = None

        size_el = product.select_one("span.product-card__price-per-unit")
        size = size_el.text.strip() if size_el else None

        yield {
            "name": name,
            "original_price": original_price,
            "discount_price": discount_price,
            "size": size,
            "category": category,
            "image_url": get_carr_image_url(product),
        }


class CarrExtractor(Extractor):
    WAIT_TIMEOUT = 30
    COOKIES_BUTTON_ID = "onetrust-reject-all-handler"
    CATEGORY_LINK_SELECTOR = ".nav-first-level-categories__slide a"
    PRODUCT_CARD_SELECTOR = ".product-card__parent"

    def __init__(self, data_source_url: str, bucket_name: str, break_early: bool = False):
        super().__init__(data_source_url, bucket_name, break_early)

    def accept_cookies(self, driver: webdriver.Chrome):
        logger.info("Rejecting cookies on Carrefour (looking for 'Reject All' button)")
        try:
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, self.COOKIES_BUTTON_ID)))
            btn = driver.find_element(By.ID, self.COOKIES_BUTTON_ID)
            if btn.is_displayed() and btn.is_enabled():
                btn.click()
                logger.info("Clicked 'Reject All' cookies consent button")
            else:
                logger.info("'Reject All' button found but not clickable")
        except Exception as e:
            logger.info(f"No 'Reject All' cookies consent dialog found or error: {e}")

    def wait_for_category_links(self, driver: webdriver.Chrome) -> List[str]:
        logger.info("Waiting for Carrefour category links to load")
        try:
            WebDriverWait(driver, self.WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, self.CATEGORY_LINK_SELECTOR))
            )
        except Exception:
            logger.error("Category links did not appear within timeout")
            self.save_debug_html(driver, "carr_categories_timeout")
            self.save_screenshot(driver, "carr_categories_timeout.png")
            return []

        elements = driver.find_elements(By.CSS_SELECTOR, self.CATEGORY_LINK_SELECTOR)
        category_names = []
        for el in elements:
            name = el.text.strip()
            href = el.get_attribute("href") or ""
            if not name or name in SKIP_CATEGORIES:
                continue
            if "/cat" not in href:
                continue
            category_names.append(name)
        logger.info(f"Found {len(category_names)} Carrefour categories: {category_names}")
        return category_names

    def click_category(self, driver: webdriver.Chrome, category_name: str):
        elements = driver.find_elements(By.CSS_SELECTOR, self.CATEGORY_LINK_SELECTOR)
        for el in elements:
            if el.text.strip() == category_name:
                driver.execute_script("arguments[0].click();", el)
                return
        raise Exception(f"Category link '{category_name}' not found in DOM")

    def navigate_back_to_main(self, driver: webdriver.Chrome) -> bool:
        driver.back()
        try:
            WebDriverWait(driver, self.WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, self.CATEGORY_LINK_SELECTOR))
            )
            return True
        except Exception:
            logger.error("Failed to navigate back to main page after category extraction")
            self.save_debug_html(driver, "carr_back_navigation_failed")
            return False

    def get_page_sources(self) -> List[Generator[Dict[str, Any], None, None]]:
        logger.info("Getting Carrefour page content")
        driver = self.initialize_driver()

        try:
            driver.get(self.data_source_url)
            self.save_debug_html(driver, "carr_initial_load")

            try:
                self.accept_cookies(driver)
            except Exception as e:
                logger.warning(f"Cookie handling failed: {e}")

            self.save_debug_html(driver, "carr_after_cookies")
            category_names = self.wait_for_category_links(driver)
            product_gen_list = []

            for category_name in category_names:
                navigated = False
                try:
                    logger.info(f"Clicking category: {category_name}")
                    self.click_category(driver, category_name)
                    navigated = True
                    try:
                        WebDriverWait(driver, self.WAIT_TIMEOUT).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, self.PRODUCT_CARD_SELECTOR))
                        )
                    except Exception:
                        logger.warning(f"No product cards found for category {category_name} within timeout")
                        self.save_debug_html(driver, f"carr_no_products_{category_name}")
                        if not self.navigate_back_to_main(driver):
                            break
                        continue
                    product_gen_list.append(extract_carr_product_data(driver.page_source, category_name))
                except Exception as e:
                    logger.error(f"Failed to extract category {category_name}: {e}")
                    self.save_debug_html(driver, f"carr_category_error_{category_name}")

                if navigated:
                    if not self.navigate_back_to_main(driver):
                        break

                if self.break_early:
                    logger.info("Break-early mode: stopping after the first category")
                    break

            logger.info(f"Extracted {len(product_gen_list)} Carrefour product data generators")
            return product_gen_list

        finally:
            driver.quit()
