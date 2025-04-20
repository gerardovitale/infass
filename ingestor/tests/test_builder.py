from datetime import datetime
from unittest.mock import patch

import pandas as pd
from data_builder import build_data_gen
from data_builder import build_df
from tests.conf_test import BasicTestCase

TEST_MODULE = "data_builder"


def mock_gen(category: str, n_records: int = 2):
    return (
        {
            "name": f"Sample Product {i}",
            "original_price": "1,58 €",
            "discount_price": "1,53 €",
            "size": f"Paquete {i} kg",
            "category": category,
        }
        for i in range(1, 1 + n_records)
    )


def mock_product_gen_list():
    return [mock_gen("test_category_1"), mock_gen("test_category_2")]


class TestDataBuilder(BasicTestCase):

    def setUp(self):
        logger_patch = patch(f"{TEST_MODULE}.logger")
        self.addCleanup(logger_patch.stop)
        self.mock_logger = logger_patch.start()

        datetime_patch = patch(f"{TEST_MODULE}.datetime")
        self.addCleanup(datetime_patch.stop)
        self.mock_datetime = datetime_patch.start()

    def test_build_df(self):
        test_product_gen = mock_gen("test_category")
        self.mock_datetime.now.return_value = datetime(2024, 12, 23)

        expected_cols = ["name", "original_price", "discount_price", "size", "category", "date"]
        expected_data = [
            ("Sample Product 1", "1,58 €", "1,53 €", "Paquete 1 kg", "test_category", "2024-12-23"),
            ("Sample Product 2", "1,58 €", "1,53 €", "Paquete 2 kg", "test_category", "2024-12-23"),
        ]
        expected_df = pd.DataFrame(expected_data, columns=expected_cols)

        actual_df = build_df(test_product_gen)
        self.assert_pandas_dataframes_equal(expected_df, actual_df)

    def test_build_data_gen(self):
        test_product_gen_list = [mock_gen("test_category_1"), mock_gen("test_category_2")]
        self.mock_datetime.now.return_value = datetime(2024, 12, 23)

        expected_cols = ["name", "original_price", "discount_price", "size", "category", "date"]
        expected_data_1 = [
            ("Sample Product 1", "1,58 €", "1,53 €", "Paquete 1 kg", "test_category_1", "2024-12-23"),
            ("Sample Product 2", "1,58 €", "1,53 €", "Paquete 2 kg", "test_category_1", "2024-12-23"),
        ]
        expected_df_1 = pd.DataFrame(expected_data_1, columns=expected_cols)

        expected_data_2 = [
            ("Sample Product 1", "1,58 €", "1,53 €", "Paquete 1 kg", "test_category_2", "2024-12-23"),
            ("Sample Product 2", "1,58 €", "1,53 €", "Paquete 2 kg", "test_category_2", "2024-12-23"),
        ]
        expected_df_2 = pd.DataFrame(expected_data_2, columns=expected_cols)

        actual = build_data_gen(test_product_gen_list)

        self.assert_pandas_dataframes_equal(expected_df_1, next(actual))
        self.assert_pandas_dataframes_equal(expected_df_2, next(actual))

    def test_build_data_gen_empty_sources(self):
        test_product_gen_list = []
        expected = []
        actual = build_data_gen(test_product_gen_list)
        self.assertEqual(expected, list(actual))
