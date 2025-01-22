from typing import List, Tuple
from unittest import TestCase

import pandas as pd


class BasicTestCase(TestCase):
    @staticmethod
    def pandas_df_to_sorted_tuple_list(df: pd.DataFrame) -> List[Tuple]:
        return list(df.fillna("null").sort_values(df.columns.to_list()).itertuples(index=False, name=None))

    def assert_pandas_dataframes_equal(self, expected_df: pd.DataFrame, actual_df: pd.DataFrame):
        self.assertIsInstance(expected_df, pd.DataFrame)
        self.assertIsInstance(actual_df, pd.DataFrame)
        assert actual_df.columns.tolist() == expected_df.columns.tolist()
        assert self.pandas_df_to_sorted_tuple_list(actual_df) == self.pandas_df_to_sorted_tuple_list(expected_df)
