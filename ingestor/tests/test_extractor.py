from unittest import TestCase
from unittest.mock import MagicMock
from unittest.mock import patch

from bs4 import BeautifulSoup
from extractor import Extractor
from extractor.carr_extractor import extract_carr_product_data
from extractor.carr_extractor import get_carr_image_url
from extractor.merc_extractor import extract_product_data
from extractor.merc_extractor import get_image_url
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

        time_patch = patch("extractor.merc_extractor.time.sleep")
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
            "image_url": "sample.jpg",
        }
        actual_output = extract_product_data(self.test_html, category)
        self.assertIsNotNone(actual_output)
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
            "image_url": "https://prod-merc.imgix.net/images/c1788076223b499bd260c6a03d89b087.jpg",
        }
        actual_output = extract_product_data(no_discount_html, category)
        self.assertIsNotNone(actual_output)
        self.assertEqual(next(actual_output), expected_output)

    def test_extract_product_data_when_there_is_no_image(self):
        category = "test_category"
        no_image_html = """
        <div data-testid="product-cell" class="product-cell product-cell--actionable">
            <button class="product-cell__content-link" data-testid="open-product-detail">
                <div class="product-cell__info">
                    <h4 class="subhead1-r product-cell__description-name" data-testid="product-cell-name">
                        No Image Product
                    </h4>
                    <div class="product-format product-format__size--cell" tabindex="0">
                        <span class="footnote1-r">Caja </span><span class="footnote1-r">12 uds</span>
                    </div>
                    <div class="product-price">
                        <div>
                            <p class="product-price__unit-price subhead1-b" data-testid="product-price">
                                5,00 €
                            </p>
                        </div>
                    </div>
                </div>
            </button>
        </div>
        """
        expected_output = {
            "name": "No Image Product",
            "original_price": "5,00 €",
            "discount_price": None,
            "size": "Caja 12 uds",
            "category": category,
            "image_url": None,
        }
        actual_output = extract_product_data(no_image_html, category)
        self.assertIsNotNone(actual_output)
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


class DummyExtractor(Extractor):
    def __init__(self, data_source_url="url", bucket_name="bucket", break_early=False):
        super().__init__(data_source_url, bucket_name, break_early)

    def get_page_sources(self):
        return []


class TestExtractorGCS(TestCase):
    def test_upload_to_gcs_calls_blob_upload(self):
        with patch("extractor.GCSClientSingleton.get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_bucket = MagicMock()
            mock_blob = MagicMock()
            mock_get_client.return_value = mock_client
            mock_client.get_bucket.return_value = mock_bucket
            mock_bucket.blob.return_value = mock_blob

            DummyExtractor.upload_to_gcs("file.png", "bucket", "dest.png")
            mock_client.get_bucket.assert_called_with("bucket")
            mock_bucket.blob.assert_called_with("dest.png")
            mock_blob.upload_from_filename.assert_called_with("file.png")

    def test_save_screenshot_calls_upload_and_saves_file(self):
        extractor = DummyExtractor("url", "bucket", False)
        driver = MagicMock()
        with patch.object(DummyExtractor, "upload_to_gcs") as mock_upload:
            extractor.save_screenshot(driver, "file.png", "bucket")
            driver.save_screenshot.assert_called_with("file.png")
            mock_upload.assert_called()


def get_carr_test_html():
    return """
        <div class="product-card__parent">
            <div class="product-card__detail">
                <h2 class="product-card__title">
                    <a class="product-card__title-link" href="/supermercado/producto/cerveza-clasica">
                        Cerveza Mahou Clásica pack de 28 latas de 33 cl.
                    </a>
                </h2>
                <span class="product-card__price">4,99 €</span>
                <span class="product-card__price-per-unit">83,17 €/kg</span>
                <img class="product-card__image"
                     src="https://static.carrefour.es/hd_350x_/img_pim_food/718559_00_1.jpg?fit=crop&h=300">
            </div>
        </div>
        """


def get_carr_discount_html():
    return """
        <div class="product-card__parent">
            <div class="product-card__detail">
                <h2 class="product-card__title">
                    <a class="product-card__title-link" href="/supermercado/producto/aceite-oliva">
                        Aceite de oliva virgen extra
                    </a>
                </h2>
                <span class="product-card__price--strikethrough">20,44 €</span>
                <span class="product-card__price--current">14,99 €</span>
                <span class="product-card__price-per-unit">1,62 €/l</span>
                <img class="product-card__image"
                     src="https://static.carrefour.es/hd_350x_/img_pim_food/aceite_001.jpg">
            </div>
        </div>
        """


class TestCarrExtractProductData(TestCase):

    def test_extract_carr_product_data(self):
        html = get_carr_test_html()
        result = list(
            extract_carr_product_data(
                html, "Bebidas", "https://www.carrefour.es", "https://www.carrefour.es/supermercado/bebidas/cat20003/c"
            )
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(
            result[0],
            {
                "name": "Cerveza Mahou Clásica pack de 28 latas de 33 cl.",
                "original_price": "4,99 €",
                "discount_price": None,
                "price_per_unit": "83,17 €/kg",
                "category": "Bebidas",
                "image_url": "https://static.carrefour.es/hd_350x_/img_pim_food/718559_00_1.jpg",
                "product_url": "https://www.carrefour.es/supermercado/producto/cerveza-clasica",
                "source_page": "https://www.carrefour.es/supermercado/bebidas/cat20003/c",
            },
        )

    def test_extract_carr_product_data_with_discount(self):
        html = get_carr_discount_html()
        result = list(extract_carr_product_data(html, "Alimentación"))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["original_price"], "20,44 €")
        self.assertEqual(result[0]["discount_price"], "14,99 €")
        self.assertEqual(result[0]["price_per_unit"], "1,62 €/l")

    def test_extract_carr_product_data_no_image(self):
        html = """
        <div class="product-card__parent">
            <div class="product-card__detail">
                <h2 class="product-card__title">
                    <a class="product-card__title-link" href="/supermercado/producto/agua">
                        Agua mineral natural
                    </a>
                </h2>
                <span class="product-card__price">0,50 €</span>
                <span class="product-card__price-per-unit">0,33 €/l</span>
            </div>
        </div>
        """
        result = list(extract_carr_product_data(html, "Bebidas"))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], "Agua mineral natural")
        self.assertIsNone(result[0]["image_url"])

    def test_extract_carr_product_data_empty_page(self):
        result = list(extract_carr_product_data("", "Bebidas"))
        self.assertEqual(result, [])

    def test_extract_carr_product_data_no_products(self):
        html = "<html><body><div>No products here</div></body></html>"
        result = list(extract_carr_product_data(html, "Bebidas"))
        self.assertEqual(result, [])


class TestGetCarrImageUrl(TestCase):

    def test_image_url_from_src(self):
        html = """
        <div class="product-card__parent">
            <img class="product-card__image"
                 src="https://static.carrefour.es/hd_350x_/img_pim_food/718559_00_1.jpg">
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        result = get_carr_image_url(soup)
        self.assertEqual(result, "https://static.carrefour.es/hd_350x_/img_pim_food/718559_00_1.jpg")

    def test_image_url_strips_query_params(self):
        html = """
        <div>
            <img class="product-card__image"
                 src="https://static.carrefour.es/img/product.jpg?fit=crop&h=300&w=300">
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        result = get_carr_image_url(soup)
        self.assertEqual(result, "https://static.carrefour.es/img/product.jpg")

    def test_image_url_no_image(self):
        html = "<div><p>No image</p></div>"
        soup = BeautifulSoup(html, "html.parser")
        result = get_carr_image_url(soup)
        self.assertIsNone(result)

    def test_image_url_data_uri_returns_none(self):
        html = '<div><img class="product-card__image" src="data:image/gif;base64,placeholder"></div>'
        soup = BeautifulSoup(html, "html.parser")
        result = get_carr_image_url(soup)
        self.assertIsNone(result)
