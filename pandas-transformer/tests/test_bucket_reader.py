from unittest.mock import (
    patch,
)

import pandas as pd

from bucket_reader import read_csv_as_pd_df
from tests.conf_test import BasicTestCase

TEST_MODULE = "bucket_reader"


class MockBlob:
    def __init__(self, name, content):
        self.name = name
        self.content = content


class TestBucketReader(BasicTestCase):

    def setUp(self):
        logger_patch = patch(f"{TEST_MODULE}.logger")
        self.addCleanup(logger_patch.stop)
        self.mock_logger = logger_patch.start()

        list_blobs_patch = patch(f"{TEST_MODULE}._list_blobs")
        self.addCleanup(list_blobs_patch.stop)
        self.mock_list_blobs = list_blobs_patch.start()

        read_blobs_patch = patch(f"{TEST_MODULE}._read_blobs")
        self.addCleanup(read_blobs_patch.stop)
        self.mock_read_blobs = read_blobs_patch.start()

    def test_read_csv_as_pd_df_should_return_last_object_when_limit_is_minus_1(self):
        test_bucket_name = 'test_bucket_name'
        limit = -1
        self.mock_list_blobs.return_value = (
            MockBlob(f"2025-01-0{day}.csv", "mock_content")
            for day in range(1, 5)
        )
        self.mock_read_blobs.return_value = "col1,col2,col3\n1,2,3\n1,2,3\n1,2,3"

        expected = pd.DataFrame({
            "col1": [1, 1, 1],
            "col2": [2, 2, 2],
            "col3": [3, 3, 3],
        })
        actual = read_csv_as_pd_df(test_bucket_name, limit)

        self.assertIsInstance(actual, pd.DataFrame)
        self.assert_pandas_dataframes_equal(expected, actual)

    def test_read_csv_as_pd_df_should_return_all_objects_when_limit_is_not_passed(self):
        test_bucket_name = 'test_bucket_name'
        self.mock_list_blobs.return_value = (
            MockBlob(f"2025-01-0{day}.csv", "mock_content")
            for day in range(1, 4)
        )
        self.mock_read_blobs.return_value = "col1,col2,col3\n1,2,3\n1,2,3\n1,2,3"

        expected = pd.DataFrame({
            "col1": [1, 1, 1, 1, 1, 1, 1, 1, 1],
            "col2": [2, 2, 2, 2, 2, 2, 2, 2, 2],
            "col3": [3, 3, 3, 3, 3, 3, 3, 3, 3],
        })
        actual = read_csv_as_pd_df(test_bucket_name)

        self.assertIsInstance(actual, pd.DataFrame)
        self.assert_pandas_dataframes_equal(expected, actual)

    def test_read_csv_as_pd_df_should_return_n_objects_when_limit_is_n(self):
        test_bucket_name = 'test_bucket_name'
        limit = 2
        self.mock_list_blobs.return_value = (
            MockBlob(f"2025-01-0{day}.csv", "mock_content")
            for day in range(1, 8)
        )
        self.mock_read_blobs.return_value = "col1,col2,col3\n1,2,3\n1,2,3\n1,2,3"

        expected = pd.DataFrame({
            "col1": [1, 1, 1, 1, 1, 1],
            "col2": [2, 2, 2, 2, 2, 2],
            "col3": [3, 3, 3, 3, 3, 3],
        })
        actual = read_csv_as_pd_df(test_bucket_name, limit)

        self.assertIsInstance(actual, pd.DataFrame)
        self.assert_pandas_dataframes_equal(expected, actual)
