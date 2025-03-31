from typing import (
    List,
    Tuple,
)
from unittest import TestCase

import pandas as pd


class BasicTestCase(TestCase):
    @staticmethod
    def pandas_df_to_sorted_tuple_list(df: pd.DataFrame) -> List[Tuple]:
        return list(df.fillna("null").sort_values(df.columns.to_list()).itertuples(index=False, name=None))

    @staticmethod
    def assert_columns_are_equal(expected_df: pd.DataFrame, actual_df: pd.DataFrame) -> None:
        assert actual_df.columns.tolist() == expected_df.columns.tolist()

    def check_is_pd_dataframe(self, expected_df: pd.DataFrame, actual_df: pd.DataFrame) -> None:
        self.assertIsInstance(expected_df, pd.DataFrame)
        self.assertIsInstance(actual_df, pd.DataFrame)

    def assert_pandas_dataframes_equal(self, expected_df: pd.DataFrame, actual_df: pd.DataFrame):
        self.check_is_pd_dataframe(expected_df, actual_df)
        self.assert_columns_are_equal(expected_df, actual_df)
        assert self.pandas_df_to_sorted_tuple_list(actual_df) == self.pandas_df_to_sorted_tuple_list(expected_df)

    def assert_pandas_dataframe_almost_equal(self, expected_df: pd.DataFrame, actual_df: pd.DataFrame):
        self.check_is_pd_dataframe(expected_df, actual_df)
        self.assert_columns_are_equal(expected_df, actual_df)
        pd.testing.assert_frame_equal(actual_df, expected_df, check_exact=False, rtol=1e-5, atol=1e-8)
