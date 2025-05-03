from typing import List
from typing import Tuple
from unittest import TestCase

import pandas as pd


class BasicTestCase(TestCase):

    def assert_pandas_dataframe_almost_equal(self, expected_df: pd.DataFrame, actual_df: pd.DataFrame) -> None:
        sorted_expected_df = expected_df.sort_values(expected_df.columns.to_list()).reset_index(drop=True)
        sorted_actual_df = actual_df.sort_values(actual_df.columns.to_list()).reset_index(drop=True)
        self.check_is_pd_dataframe(sorted_expected_df, sorted_actual_df)
        self.assert_columns_are_equal(sorted_expected_df, sorted_actual_df)
        self.print_dfs(sorted_expected_df, sorted_actual_df)
        pd.testing.assert_frame_equal(
            left=sorted_expected_df,
            right=sorted_actual_df,
            check_exact=False,
            rtol=1e-5,
            atol=1e-8,
        )

    def assert_pandas_dataframes_equal(self, expected_df: pd.DataFrame, actual_df: pd.DataFrame) -> None:
        self.check_is_pd_dataframe(expected_df, actual_df)
        self.assert_columns_are_equal(expected_df, actual_df)
        self.print_dfs(expected_df, actual_df)
        assert self.pandas_df_to_sorted_tuple_list(actual_df) == self.pandas_df_to_sorted_tuple_list(expected_df)

    @staticmethod
    def pandas_df_to_sorted_tuple_list(df: pd.DataFrame) -> List[Tuple]:
        return list(df.fillna("null").sort_values(df.columns.to_list()).itertuples(index=False, name=None))

    @staticmethod
    def assert_columns_are_equal(expected_df: pd.DataFrame, actual_df: pd.DataFrame) -> None:
        assert actual_df.columns.tolist() == expected_df.columns.tolist()

    @staticmethod
    def print_dfs(expected_df: pd.DataFrame, actual_df: pd.DataFrame) -> None:
        print("\nEXPECTED_DF (left)\n", expected_df.to_string(index=False), "\n")
        print("\nACTUAL_DF (right)\n", actual_df.to_string(index=False), "\n")

    def check_is_pd_dataframe(self, expected_df: pd.DataFrame, actual_df: pd.DataFrame) -> None:
        self.assertIsInstance(expected_df, pd.DataFrame)
        self.assertIsInstance(actual_df, pd.DataFrame)
