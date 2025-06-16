import numpy as np
import pandas as pd
from schema import PD_MERC_SCHEMA
from tests.conf_test import BasicTestCase

from transformer import add_price_column
from transformer import add_price_moving_average
from transformer import cast_price_columns_as_float32
from transformer import deduplicate_products_with_diff_prices_per_date
from transformer import split_category_subcategory
from transformer import standardize_string_columns
from transformer import transform


class TestIntegrationTransformer(BasicTestCase):

    def test_transformer(self):
        test_df = pd.read_csv("tests/test-data-source/integration_input_data.csv")
        expected_df = pd.read_csv("tests/test-data-source/integration_expected_data.csv").astype(PD_MERC_SCHEMA)
        actual_df = transform(test_df)
        self.assert_pandas_dataframe_almost_equal(expected_df, actual_df)


class TestSMA(BasicTestCase):
    def test_add_price_moving_average_when_there_just_7_records(self):
        test_records_number = 7
        test_df = pd.DataFrame(
            {
                "date": [f"2025-04-0{n}" for n in range(1, test_records_number + 1)],
                "name": ["test_name"] * test_records_number,
                "size": ["test_size"] * test_records_number,
                "price": [float(n) for n in range(1, test_records_number + 1)],
            }
        )

        expected_df = test_df.copy()
        expected_df["price_ma_7"] = [None, None, None, None, None, None, 4.00]
        expected_df["price_ma_15"] = [None] * test_records_number
        expected_df["price_ma_30"] = [None] * test_records_number
        expected_df = expected_df.astype(
            {
                "price_ma_7": "float32",
                "price_ma_15": "float32",
                "price_ma_30": "float32",
            }
        )

        actual_df = add_price_moving_average(test_df)
        self.assert_pandas_dataframe_almost_equal(expected_df, actual_df)

    def test_add_price_moving_average(self):
        test_df = pd.read_csv("tests/test-data-source/add_moving_average_input_data.csv")
        expected_df = pd.read_csv("tests/test-data-source/add_moving_average_expected_data.csv").astype(
            {
                "price_ma_7": "float32",
                "price_ma_15": "float32",
                "price_ma_30": "float32",
            }
        )
        actual_df = add_price_moving_average(test_df)
        self.assert_pandas_dataframe_almost_equal(expected_df, actual_df)


class TestTransformer(BasicTestCase):

    def test_cast_price_columns_as_float32(self):
        test_df = pd.DataFrame(
            {
                "original_price": ["€12,99", "€3,50", None, "5.99"],
                "discount_price": ["€10,99", "€2,99", None, "5.99"],
            }
        )

        expected_df = pd.DataFrame(
            {
                "original_price": [12.99, 3.50, None, 5.99],
                "discount_price": [10.99, 2.99, None, 5.99],
            }
        ).astype(
            {
                "original_price": "float32",
                "discount_price": "float32",
            }
        )

        actual_df = cast_price_columns_as_float32(test_df)
        self.assert_pandas_dataframe_almost_equal(expected_df, actual_df)

    def test_split_category_subcategory(self):
        test_df = pd.DataFrame(
            {
                "category": ["Food > Snacks", "Beverages > Coffee", "Household > Cleaning", "Electronics", None],
            }
        )

        expected_df = pd.DataFrame(
            {
                "category": ["Food", "Beverages", "Household", "Electronics", None],
                "subcategory": ["Snacks", "Coffee", "Cleaning", None, None],
            }
        )

        actual_df = split_category_subcategory(test_df)
        self.assert_pandas_dataframes_equal(expected_df, actual_df)

    def test_standardize_string_columns(self):
        test_df = pd.DataFrame(
            {
                "name": ["  Ápple  ", "piñá"],
                "size": ["  SMALL ", "gránde"],
                "category": ["  Frutás ", "bebídas"],
                "subcategory": ["  TROPICAL ", "azúcares"],
            }
        )

        expected_df = pd.DataFrame(
            {
                "name": ["apple", "pina"],
                "size": ["small", "grande"],
                "category": ["frutas", "bebidas"],
                "subcategory": ["tropical", "azucares"],
            }
        ).astype(
            {
                "name": "string",
                "size": "string",
                "category": "category",
                "subcategory": "category",
            }
        )

        actual_df = standardize_string_columns(test_df)
        self.assert_pandas_dataframe_almost_equal(expected_df, actual_df)

    def test_deduplicate_products_with_diff_prices_per_date(self):
        test_df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-01", "2024-01-01", "2024-01-02"],
                "name": ["apple", "apple", "banana", "apple"],
                "size": ["1kg", "1kg", "1kg", "1kg"],
                "category": ["fruit", "fruit", "fruit", "fruit"],
                "subcategory": ["fresh", "fresh", "fresh", "fresh"],
                "price": [1.99, 2.09, 0.99, 2.19],
            }
        )

        expected_df = test_df.copy()
        expected_df["dedup_id"] = [1, 2, 1, 1]
        expected_df["dedup_id"] = expected_df["dedup_id"].astype("int8")

        actual_df = deduplicate_products_with_diff_prices_per_date(test_df)
        self.assert_pandas_dataframe_almost_equal(expected_df, actual_df)

    def test_deduplicate_products_with_diff_prices_per_date_when_subcategory_has_nan_values(self):
        test_df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-01", "2024-01-01", "2024-01-02"],
                "name": ["apple", "apple", "apple", "apple"],
                "size": ["1kg", "1kg", "1kg", "1kg"],
                "category": ["fruit", "fruit", "fruit", "fruit"],
                "subcategory": ["fresh", "fresh", "fresh", np.nan],
                "price": [1.99, 2.09, 2.99, 2.19],
            }
        )

        expected_df = test_df.copy()
        expected_df["dedup_id"] = [1, 2, 3, 1]
        expected_df["dedup_id"] = expected_df["dedup_id"].astype("int8")

        actual_df = deduplicate_products_with_diff_prices_per_date(test_df)

        self.assert_pandas_dataframe_almost_equal(expected_df, actual_df)

    def test_add_price_column(self):
        test_df = pd.DataFrame(
            {
                "original_price": [1.99, 2.09, 2.99],
                "discount_price": [0.99, None, np.nan],
            }
        )
        expected_df = pd.DataFrame(
            {
                "original_price": [1.99, 2.09, 2.99],
                "discount_price": [0.99, None, np.nan],
                "price": [0.99, 2.09, 2.99],
            }
        )
        actual_df = add_price_column(test_df)
        self.assert_pandas_dataframes_equal(expected_df, actual_df)
