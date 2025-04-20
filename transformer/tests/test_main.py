import os
from unittest import TestCase
from unittest.mock import patch

import pandas as pd
from main import run_data_transformation

TEST_MODULE = "main"


class TestRunDataTransformation(TestCase):

    def setUp(self):
        logger_patch = patch(f"{TEST_MODULE}.logging")
        self.addCleanup(logger_patch.stop)
        self.mock_logger = logger_patch.start()

        csv_reader_patch = patch(f"{TEST_MODULE}.read_csv_as_pd_df")
        self.addCleanup(csv_reader_patch.stop)
        self.mock_csv_reader = csv_reader_patch.start()

        transformer_patch = patch(f"{TEST_MODULE}.transformer")
        self.addCleanup(transformer_patch.stop)
        self.mock_transformer = transformer_patch.start()

        bigquery_writer_patch = patch(f"{TEST_MODULE}.write_to_bigquery")
        self.addCleanup(bigquery_writer_patch.stop)
        self.mock_bigquery_writer = bigquery_writer_patch.start()

    def test_remote_run_with_limit(self):
        test_params = {
            "bucket_data_source": "my_bucket_name",
            "destination_table": "myproject.dataset.table",
            "transformer_limit": "5",
            "is_local_run": None,
        }

        mock_df = pd.DataFrame({"col": [1, 2, 3]})
        self.mock_csv_reader.return_value = mock_df
        self.mock_transformer.return_value = mock_df

        run_data_transformation(**test_params)

        self.mock_csv_reader.assert_called_once_with("my_bucket_name", 5)
        self.mock_transformer.assert_called_once_with(mock_df)
        self.mock_bigquery_writer.assert_called_once_with(mock_df, "myproject", "dataset", "table")

    def test_local_run_with_limit_should_save_csv(self):
        test_params = {
            "bucket_data_source": "my_bucket_name",
            "destination_table": "myproject.dataset.table",
            "transformer_limit": "5",
            "is_local_run": True,
        }
        expected_csv_dest_path = "data/myproject_dataset_table.csv"

        mock_df = pd.DataFrame({"col": [1, 2, 3]})
        self.mock_csv_reader.return_value = mock_df
        self.mock_transformer.return_value = mock_df

        run_data_transformation(**test_params)

        self.mock_csv_reader.assert_called_once_with("my_bucket_name", 5)
        self.mock_transformer.assert_called_once_with(mock_df)
        self.mock_bigquery_writer.assert_not_called()

        # Check if the DataFrame is saved as a CSV file
        assert os.path.exists(expected_csv_dest_path)
        # Clean up
        os.remove(expected_csv_dest_path)

    def test_remote_run_without_limit(self):
        test_params = {
            "bucket_data_source": "my_bucket_name",
            "destination_table": "myproject.dataset.table",
            "transformer_limit": None,
            "is_local_run": None,
        }

        mock_df = pd.DataFrame({"col": [1, 2, 3]})
        self.mock_csv_reader.return_value = mock_df
        self.mock_transformer.return_value = mock_df

        run_data_transformation(**test_params)

        self.mock_csv_reader.assert_called_once_with("my_bucket_name", None)
        self.mock_transformer.assert_called_once_with(mock_df)
        self.mock_bigquery_writer.assert_called_once_with(mock_df, "myproject", "dataset", "table")

    def test_run_with_invalid_limit(self):
        test_params = {
            "bucket_data_source": "my_bucket_name",
            "destination_table": "myproject.dataset.table",
            "transformer_limit": "invalid_limit",
            "is_local_run": None,
        }

        with self.assertRaises(ValueError):
            run_data_transformation(**test_params)

        self.mock_csv_reader.assert_not_called()
        self.mock_transformer.assert_not_called()
        self.mock_bigquery_writer.assert_not_called()
