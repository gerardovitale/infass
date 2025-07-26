import logging
import time

from extractor import Extractor
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

logger = logging.getLogger(__name__)


class CarrExtractor(Extractor):
    WAIT_CONTENT_TIME_SLEEP = 2
    COOKIES_BUTTON_ID = "onetrust-reject-all-handler"

    def __init__(self, data_source_url: str, bucket_name: str, test_mode: bool):
        super().__init__(data_source_url, bucket_name, test_mode)

    def accept_cookies(self, driver):
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

    def get_page_sources(self):
        logger.info("Getting page content")
        driver = self.initialize_driver()

        try:
            driver.get(self.data_source_url)
            time.sleep(self.WAIT_CONTENT_TIME_SLEEP)
            self.accept_cookies(driver)

            # Save page source after menu click for debugging
            with open("data/tmp/carrefour-debug.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            logger.info("Saved page source after menu click to data/tmp/carrefour-debug.html")

            time.sleep(60)
        finally:
            driver.quit()
