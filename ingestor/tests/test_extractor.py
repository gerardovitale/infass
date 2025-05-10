from unittest import TestCase
from unittest.mock import patch

from app.extractor import extract_product_data
from bs4 import BeautifulSoup
from extractor import get_image_url
from tests.conf_test import BasicTestCase


def get_test_html():
    return """
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
                            <p class="product-price__unit-price subhead1-b product-price__unit-price--discount"
                            data-testid="product-price">
                                1,53 €
                            </p>
                            <p class="product-price__extra-price subhead1-r"> /ud.</p>
                        </div>
                    </div>
                </div>
            </button>
        </div>
        """


class TestExtractor(BasicTestCase):
    def setUp(self):
        logger_patch = patch("extractor.logger")
        self.addCleanup(logger_patch.stop)
        self.mock_logger = logger_patch.start()

        time_patch = patch("extractor.time.sleep")
        self.addCleanup(time_patch.stop)
        self.mock_sleep = time_patch.start()

        self.test_html = get_test_html()

    def test_extract_product_data(self):
        category = "test_category"
        expected_output = {
            "name": "Sample Product",
            "original_price": "1,58 €",
            "discount_price": "1,53 €",
            "size": "Paquete 1 kg",
            "category": category,
            "image": "sample.jpg",
        }
        actual_output = extract_product_data(self.test_html, category)
        self.assertEqual(next(actual_output), expected_output)

    def test_extract_product_data_when_there_is_no_discount(self):
        category = "test_category"
        no_discount_html = """
        <div data-testid="product-cell" class="product-cell product-cell--actionable">
            <button class="product-cell__content-link" data-testid="open-product-detail">
                <div class="product-cell__image-wrapper" aria-hidden="true">
                    <img alt="Aceite de oliva 0,4º Hacendado"
                         src="https://prod-merc.imgix.net/images/c1788076223b499bd260c6a03d89b087.jpg?fit=crop&amp;h=300&amp;w=300"
                         loading="lazy">
                    <span class="product-cell__image-overlay"></span>
                </div>
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

        expected_output = {
            "name": "No Discount Product",
            "original_price": "2,00 €",
            "discount_price": None,
            "size": "Unidad 500 ml",
            "category": category,
            "image": "https://prod-merc.imgix.net/images/c1788076223b499bd260c6a03d89b087.jpg",
        }
        actual_output = extract_product_data(no_discount_html, category)
        self.assertEqual(next(actual_output), expected_output)

    def test_extract_product_data_when_there_is_no_image(self):
        category = "test_category"
        no_image_html = """
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
        expected_output = {
            "name": "No Discount Product",
            "original_price": "2,00 €",
            "discount_price": None,
            "size": "Unidad 500 ml",
            "category": category,
            "image": None,
        }
        actual_output = extract_product_data(no_image_html, category)
        self.assertEqual(next(actual_output), expected_output)


class TestGetImageUrl(TestCase):

    def test_image_with_query_string(self):
        html = '<div><img src="https://example.com/image.jpg?fit=crop&h=300&w=300"></div>'
        soup = BeautifulSoup(html, "html.parser")
        result = get_image_url(soup)
        assert result == "https://example.com/image.jpg"

    def test_image_without_query_string(self):
        html = '<div><img src="https://example.com/image.jpg"></div>'
        soup = BeautifulSoup(html, "html.parser")
        result = get_image_url(soup)
        assert result == "https://example.com/image.jpg"

    def test_no_image_tag(self):
        html = "<div><p>No image here</p></div>"
        soup = BeautifulSoup(html, "html.parser")
        result = get_image_url(soup)
        assert result is None

    def test_image_tag_without_src(self):
        html = '<div><img alt="no src attribute"></div>'
        soup = BeautifulSoup(html, "html.parser")
        result = get_image_url(soup)
        assert result is None
