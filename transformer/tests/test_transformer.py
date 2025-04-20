import numpy as np
import pandas as pd
from schema import (
    PD_MERC_SCHEMA,
)
from tests.conf_test import BasicTestCase

from transformer import add_price_column
from transformer import cast_price_columns_as_float32
from transformer import create_size_pattern_column
from transformer import deduplicate_products_with_diff_prices_per_date
from transformer import split_category_subcategory
from transformer import standardize_size_columns
from transformer import standardize_string_columns
from transformer import transformer


class TestIntegrationTransformer(BasicTestCase):

    def test_transformer(self):
        test_df = pd.read_csv("tests/test-data-source/raw_data.csv")
        expected_df = pd.read_csv("tests/test-data-source/expected_data.csv").astype(PD_MERC_SCHEMA)
        actual_df = transformer(test_df)
        print(actual_df["category"].unique())
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


class TestSizeTransformer(BasicTestCase):
    def test_standardize_size_column_when_category_is_water_and_soft_drinks(self):
        test_data = [
            # single container
            "lata 500 ml",
            "botella 1,5 l",
            "garrafa 6 l",
            "brick 1 l",
            "botellin 250 ml",
            # multiple container
            "12 latas x 330 ml",
            "6 botellas x 1,5 l",
            "6 botellines x 250 ml",
            "3 ud. x 32 g",
            # "caja 12 sobres (36 g)",
        ]
        test_df = pd.DataFrame(test_data, columns=["size"])

        expected_columns = ["container_n", "container", "quantity", "unit"]
        expected_data = [
            # single container
            (1, "lata", 500, "ml"),
            (1, "botella", 1.5, "l"),
            (1, "garrafa", 6, "l"),
            (1, "brick", 1, "l"),
            (1, "botellin", 250, "ml"),
            # multiple container
            (12, "latas", 330, "ml"),
            (6, "botellas", 1.5, "l"),
            (6, "botellines", 250, "ml"),
            (3, "ud.", 32, "g"),
            # (12, "sobres", 36, "g"),
        ]
        expected_df = pd.DataFrame(expected_data, columns=expected_columns)

        actual_df = standardize_size_columns(test_df)
        self.assert_pandas_dataframes_equal(actual_df, expected_df)

    def test_create_size_pattern_column_when_category_is_water_and_soft_drinks(self):
        test_data = [
            # single container
            "lata 500 ml",
            "botella 1,5 l",
            "garrafa 6 l",
            "brick 1 l",
            "botellin 250 ml",
            # multiple container
            "12 latas x 330 ml",
            "6 botellas x 1,5 l",
            "6 botellines x 250 ml",
            "3 ud. x 32 g",
            "caja 12 sobres (36 g)",
        ]
        test_df = pd.DataFrame(test_data, columns=["size"])

        expected_columns = ["size", "size_pattern"]
        expected_data = [
            # single container
            (
                "lata 500 ml",
                "container x unit",
            ),
            (
                "botella 1,5 l",
                "container x unit",
            ),
            (
                "garrafa 6 l",
                "container x unit",
            ),
            (
                "brick 1 l",
                "container x unit",
            ),
            (
                "botellin 250 ml",
                "container x unit",
            ),
            # multiple container
            ("12 latas x 330 ml", "x container x unit"),
            ("6 botellas x 1,5 l", "x container x unit"),
            ("6 botellines x 250 ml", "x container x unit"),
            ("3 ud. x 32 g", "x container x unit"),
            ("caja 12 sobres (36 g)", "container x container x unit"),
        ]
        expected_df = pd.DataFrame(expected_data, columns=expected_columns)

        actual_df = create_size_pattern_column(test_df)
        self.assert_pandas_dataframes_equal(actual_df, expected_df)
