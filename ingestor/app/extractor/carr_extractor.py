from __future__ import annotations

import itertools
import logging
from typing import Any
from typing import Dict
from typing import Generator
from typing import List
from typing import Optional
from typing import Tuple
from urllib.parse import urlparse
from urllib.parse import urlunparse

from bs4 import BeautifulSoup
from extractor import Extractor
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from timing import timed_phase

logger = logging.getLogger(__name__)

SKIP_CATEGORIES = {"Mis productos", "Ofertas"}


def get_carr_image_url(product_soup: BeautifulSoup) -> Optional[str]:
    image = product_soup.find("img", class_="product-card__image")
    if not image:
        return None
    src = image.get("src")
    if not src or not isinstance(src, str):
        return None
    parsed = urlparse(src.strip())
    if parsed.scheme == "data":
        return None
    return urlunparse(parsed._replace(query="", fragment=""))


def extract_carr_product_data(
    page_source: str, category: str, base_url: str = "", source_page: str = ""
) -> Generator[Dict[str, Any], None, None]:
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

        price_per_unit_el = product.select_one("span.product-card__price-per-unit")
        price_per_unit = price_per_unit_el.text.strip() if price_per_unit_el else None

        href = title_link.get("href")
        product_url = (base_url + href) if href else None

        yield {
            "name": name,
            "original_price": original_price,
            "discount_price": discount_price,
            "price_per_unit": price_per_unit,
            "category": category,
            "image_url": get_carr_image_url(product),
            "product_url": product_url,
            "source_page": source_page,
        }


class CarrExtractor(Extractor):
    WAIT_TIMEOUT = 30
    COOKIES_BUTTON_ID = "onetrust-reject-all-handler"
    CATEGORY_LINK_SELECTOR = ".nav-first-level-categories__slide a"
    SUBCATEGORY_LINK_SELECTOR = ".nav-second-level-categories__slide a"
    PRODUCT_CARD_SELECTOR = ".product-card__parent"
    PAGINATION_NEXT_SELECTOR = ".pagination__next"
    PAGINATION_SELECTOR = ".pagination"

    def __init__(self, data_source_url: str, bucket_name: str, break_early: bool = False, is_test_mode: bool = False):
        super().__init__(data_source_url, bucket_name, break_early, is_test_mode)
        parsed = urlparse(data_source_url)
        self.base_url = f"{parsed.scheme}://{parsed.netloc}"

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

    def get_category_links(self, driver: webdriver.Chrome) -> List[Tuple[str, str]]:
        """Return (name, href) tuples for top-level categories."""
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
        categories = []
        for el in elements:
            name = el.text.strip()
            href = el.get_attribute("href") or ""
            if not name or name in SKIP_CATEGORIES:
                continue
            if "/cat" not in href:
                continue
            categories.append((name, href))
        logger.info(f"Found {len(categories)} Carrefour categories: {[c[0] for c in categories]}")
        return categories

    def get_subcategory_links(self, driver: webdriver.Chrome) -> List[Tuple[str, str]]:
        """Return (name, href) tuples for second-level subcategories, if any."""
        elements = driver.find_elements(By.CSS_SELECTOR, self.SUBCATEGORY_LINK_SELECTOR)
        subcategories = []
        for el in elements:
            name = el.text.strip()
            href = el.get_attribute("href") or ""
            if name and href:
                subcategories.append((name, href))
        if subcategories:
            logger.info(f"Found {len(subcategories)} subcategories: {[s[0] for s in subcategories]}")
        return subcategories

    def get_all_page_sources(self, driver: webdriver.Chrome) -> List[Tuple[str, str]]:
        """Collect (page_url, page_source) tuples from all paginated pages of the current category."""
        pages = []
        while True:
            pages.append((driver.current_url, driver.page_source))
            try:
                next_span = driver.find_element(By.CSS_SELECTOR, self.PAGINATION_NEXT_SELECTOR)
                # The href lives on the parent <a> that wraps the span; a bare <span> means last page
                parent = next_span.find_element(By.XPATH, "..")
                if parent.tag_name != "a":
                    break
                next_url = parent.get_attribute("href")
                if not next_url:
                    break
                logger.info(f"Navigating to next page: {next_url}")
                driver.get(next_url)
                WebDriverWait(driver, self.WAIT_TIMEOUT).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, self.PAGINATION_SELECTOR))
                )
            except Exception:
                break
        logger.info(f"Collected {len(pages)} page(s) for category")
        return pages

    def _wait_for_products(self, driver: webdriver.Chrome, label: str) -> bool:
        """Wait for product cards to appear. Returns True if products found."""
        try:
            WebDriverWait(driver, self.WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, self.PRODUCT_CARD_SELECTOR))
            )
            # Wait for the full product list to render (pagination appears once all products load).
            # Categories with fewer than 24 products may not have pagination; ignore the timeout.
            try:
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, self.PAGINATION_SELECTOR))
                )
            except Exception:
                pass
            return True
        except Exception:
            logger.warning(f"No product cards found for {label} within timeout")
            self.save_debug_html(driver, f"carr_no_products_{label}")
            return False

    def _extract_products_from_current_page(
        self, driver: webdriver.Chrome, category_label: str
    ) -> Generator[Dict[str, Any], None, None]:
        """Collect all paginated pages and return a chained product generator."""
        pages = self.get_all_page_sources(driver)
        return itertools.chain.from_iterable(
            extract_carr_product_data(page_source, category_label, self.base_url, page_url)
            for page_url, page_source in pages
        )

    @timed_phase("extraction")
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
            categories = self.get_category_links(driver)
            product_gen_list = []
            stop = False

            for category_name, category_href in categories:
                try:
                    logger.info(f"Navigating to category: {category_name} ({category_href})")
                    driver.get(category_href)

                    # Check for subcategories before waiting for products
                    try:
                        WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, self.SUBCATEGORY_LINK_SELECTOR))
                        )
                    except Exception:
                        pass

                    subcategories = self.get_subcategory_links(driver)

                    if subcategories:
                        for subcat_name, subcat_href in subcategories:
                            category_label = f"{category_name} > {subcat_name}"
                            logger.info(f"Navigating to subcategory: {category_label} ({subcat_href})")
                            driver.get(subcat_href)

                            if not self._wait_for_products(driver, category_label):
                                continue

                            product_gen_list.append(self._extract_products_from_current_page(driver, category_label))

                            if self.break_early:
                                logger.info("Break-early mode: stopping after the first subcategory")
                                stop = True
                                break
                    else:
                        if not self._wait_for_products(driver, category_name):
                            continue

                        product_gen_list.append(self._extract_products_from_current_page(driver, category_name))

                except Exception as e:
                    logger.error(f"Failed to extract category {category_name}: {e}")
                    self.save_debug_html(driver, f"carr_category_error_{category_name}")

                if stop or self.break_early:
                    break

            logger.info(f"Extracted {len(product_gen_list)} Carrefour product data generators")
            return product_gen_list

        finally:
            driver.quit()
