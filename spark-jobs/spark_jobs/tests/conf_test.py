from __future__ import annotations

import json
from typing import (
    List,
    Tuple,
)
from unittest import TestCase

import pandas as pd
from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.types import StructType


class BasicTestCase(TestCase):
    @staticmethod
    def pandas_df_to_sorted_tuple_list(df: pd.DataFrame) -> List[Tuple]:
        return list(df.fillna("null").sort_values(df.columns.to_list()).itertuples(index=False, name=None))

    def assert_pandas_dataframes_equal(self, expected_df: pd.DataFrame, actual_df: pd.DataFrame):
        self.assertIsInstance(expected_df, pd.DataFrame)
        self.assertIsInstance(actual_df, pd.DataFrame)
        assert actual_df.columns.tolist() == expected_df.columns.tolist()
        assert self.pandas_df_to_sorted_tuple_list(actual_df) == self.pandas_df_to_sorted_tuple_list(expected_df)


class SparkTestCase(BasicTestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark: SparkSession = cls.setup_test_spark_session()
        cls.maxDiff = None

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.spark: SparkSession = None

    def create_empty_df(self, schema: StructType) -> DataFrame:
        return self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema)

    @staticmethod
    def setup_test_spark_session():
        return (
            SparkSession.builder.master("local[*]")
            .appName("finass-tests")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "1g")
            .getOrCreate()
        )

    def assert_spark_dataframes_equal(self, expected_df: DataFrame, actual_df: DataFrame):
        def get_spark_df_schema_as_dict(df: DataFrame) -> dict:
            return json.loads(df.schema.json())

        assert actual_df is not None, "The actual_df is None"
        assert get_spark_df_schema_as_dict(actual_df) == get_spark_df_schema_as_dict(expected_df)
        expected_pd_df, actual_pd_df = expected_df.toPandas(), actual_df.toPandas()
        self.assert_pandas_dataframes_equal(expected_pd_df, actual_pd_df)
