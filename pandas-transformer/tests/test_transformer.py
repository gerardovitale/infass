from datetime import date

import pandas as pd
from schema import DELTA_SCHEMA, INGESTION_SCHEMA
from tests.conf_test import BasicTestCase
from transformer import transformer


class TestIntegration(BasicTestCase):

    def test_transformer(self):
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
