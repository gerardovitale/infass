from datetime import date

import pandas as pd

from schema import (
    DELTA_SCHEMA,
    INGESTION_SCHEMA,
)
from tests.conf_test import BasicTestCase
from transformer import (
    transformer,
    standardize_size_columns,
    create_size_pattern_column,
)


class TestIntegration(BasicTestCase):

    def test_transformer(self):
        # COLUMNS: name, original_price, discount_price, size, category, date
        test_data = [
            ("Aceite", "6,75 €", None, "Botella 1 L", "Aceite > Aceite", date(2024, 11, 20)),
            ("Aceite", "7,75 €", None, "Botella 1 L", "Aceite > Aceite", date(2024, 11, 21)),
            ("Aceite", "8,75 €", None, "Botella 1 L", "Aceite > Aceite", date(2024, 11, 22)),
            ("Monster", "1,79 €", "1,45 €", "Lata 500 ml", "Refrescos > Isotónico", date(2024, 11, 20)),
            ("Monster", "1,85 €", "1,79 €", "Lata 500 ml", "Refrescos > Isotónico", date(2024, 11, 21)),
            ("Monster", "1,85 €", "1,79 €", "Lata 500 ml", "Refrescos > Isotónico", date(2024, 11, 22)),
        ]
        test_df = pd.DataFrame(test_data, columns=INGESTION_SCHEMA)

        expected_data = [
            (date(2024, 11, 20), 1, "aceite", "botella 1 l", "aceite", "aceite", 6.75, None, None, None, None, None),
            (
                date(2024, 11, 21),
                1,
                "aceite",
                "botella 1 l",
                "aceite",
                "aceite",
                7.75,
                6.75,
                None,
                None,
                (7.75 / 6.75) - 1,
                1.0,
            ),
            (
                date(2024, 11, 22),
                1,
                "aceite",
                "botella 1 l",
                "aceite",
                "aceite",
                8.75,
                7.75,
                None,
                None,
                (8.75 / 7.75) - 1,
                1.0,
            ),
            (
                date(2024, 11, 20),
                1,
                "monster",
                "lata 500 ml",
                "refrescos",
                "isotonico",
                1.79,
                None,
                1.45,
                False,
                None,
                None,
            ),
            (
                date(2024, 11, 21),
                1,
                "monster",
                "lata 500 ml",
                "refrescos",
                "isotonico",
                1.85,
                1.79,
                1.79,
                True,
                (1.85 / 1.79) - 1,
                1.85 - 1.79,
            ),
            (
                date(2024, 11, 22),
                1,
                "monster",
                "lata 500 ml",
                "refrescos",
                "isotonico",
                1.85,
                1.85,
                1.79,
                False,
                0.00,
                0.00,
            ),
        ]
        expected_df = pd.DataFrame(expected_data, columns=DELTA_SCHEMA)

        actual_df = transformer(test_df)

        print("\n", expected_df.to_string(index=False), "\n")
        print("\n", actual_df.to_string(index=False), "\n")

        self.assert_pandas_dataframes_equal(actual_df, expected_df)


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
            ("lata 500 ml", "container x unit",),
            ("botella 1,5 l", "container x unit",),
            ("garrafa 6 l", "container x unit",),
            ("brick 1 l", "container x unit",),
            ("botellin 250 ml", "container x unit",),

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
