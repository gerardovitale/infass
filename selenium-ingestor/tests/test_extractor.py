from typing import Dict
from unittest.mock import patch

import pandas as pd

from app.extractor import build_df
from app.extractor import extract_product_data
from tests.conf_test import BasicTestCase


class TestMercadonaExtractor(BasicTestCase):
    def setUp(self):
        logger_patch = patch("extractor.logger")
        self.addCleanup(logger_patch.stop)
        self.mock_logger = logger_patch.start()

        self.sample_html = """
        <div data-testid="product-cell" class="product-cell product-cell--actionable">
            <button class="product-cell__content-link" data-testid="open-product-detail">
                <div class="product-cell__image-wrapper" aria-hidden="true">
                    <img alt="Sample Product" src="sample.jpg" loading="lazy">
                    <span class="product-cell__image-overlay"></span>
                </div>
                <div class="product-cell__info">
                    <h4 class="subhead1-r product-cell__description-name" data-testid="product-cell-name">
                        Sample Product
                    </h4>
                    <div class="product-format product-format__size--cell" tabindex="0">
                        <span class="footnote1-r">Paquete </span><span class="footnote1-r">1 kg</span>
                    </div>
                    <div class="product-price">
                        <div>
                            <p class="product-price__previous-unit-price footnote1-r" data-testid="product-price">
                                1,58 €
                            </p>
                            <p class="product-price__unit-price subhead1-b product-price__unit-price--discount" data-testid="product-price">
                                1,53 €
                            </p>
                            <p class="product-price__extra-price subhead1-r"> /ud.</p>
                        </div>
                    </div>
                </div>
            </button>
        </div>
        """

    def test_extract_product_data(self):
        category = "test_category"
        expected_output = [
            {
                "name": "Sample Product",
                "original_price": "1,58 €",
                "discount_price": "1,53 €",
                "size": "Paquete 1 kg",
                "category": category
            }
        ]
        actual_output = extract_product_data(self.sample_html, category)
        self.assertEqual(actual_output, expected_output)

    def test_extract_product_data_when_there_is_no_discount(self):
        category = "test_category"
        no_discount_html = """
        <div data-testid="product-cell" class="product-cell product-cell--actionable">
            <button class="product-cell__content-link" data-testid="open-product-detail">
                <div class="product-cell__info">
                    <h4 class="subhead1-r product-cell__description-name" data-testid="product-cell-name">
                        No Discount Product
                    </h4>
                    <div class="product-format product-format__size--cell" tabindex="0">
                        <span class="footnote1-r">Unidad </span><span class="footnote1-r">500 ml</span>
                    </div>
                    <div class="product-price">
                        <div>
                            <p class="product-price__unit-price subhead1-b" data-testid="product-price">
                                2,00 €
                            </p>
                        </div>
                    </div>
                </div>
            </button>
        </div>
        """

        expected_output = [
            {
                "name": "No Discount Product",
                "original_price": "2,00 €",
                "discount_price": None,
                "size": "Unidad 500 ml",
                "category": category
            }
        ]
        actual_output = extract_product_data(no_discount_html, category)
        self.assertEqual(actual_output, expected_output)

    @patch("extractor.extract_product_data")
    def test_build_df_empty_sources(self, mock_extract_product_data):
        mock_page_sources: Dict[str, str] = {}
        mock_extract_product_data.return_value = []
        expected_df = pd.DataFrame()
        actual_df = build_df(mock_page_sources)
        self.assert_pandas_dataframes_equal(expected_df, actual_df)
