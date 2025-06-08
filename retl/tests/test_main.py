import os
import sqlite3
import tempfile
from unittest import mock

import pandas as pd
from main import fetch_from_bigquery
from main import upload_to_gcs
from main import write_to_sqlite


# --------------------------
# Test: write_to_sqlite
# --------------------------
def test_write_to_sqlite_creates_file():
    test_df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    expected_df = test_df.copy()

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        write_to_sqlite(test_df, db_path)

        conn = sqlite3.connect(db_path)
        actual_df = pd.read_sql_query("SELECT * FROM products", conn)
        conn.close()

        pd.testing.assert_frame_equal(actual_df, expected_df)


# --------------------------
# Test: fetch_from_bigquery
@mock.patch("main.bigquery.Client")
def test_fetch_from_bigquery(mock_bigquery_client):
    mock_query_job = mock.Mock()
    mock_query_job.to_dataframe.return_value = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})

    mock_client_instance = mock.Mock()
    mock_client_instance.query.return_value = mock_query_job
    mock_bigquery_client.return_value = mock_client_instance

    result_df = fetch_from_bigquery("my-project", "my_dataset", "my_table")

    mock_bigquery_client.assert_called_once_with(project="my-project")
    mock_client_instance.query.assert_called_once_with("SELECT * FROM `my-project.my_dataset.my_table`")
    pd.testing.assert_frame_equal(result_df, pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}))


# --------------------------
# Test: upload_to_gcs
# --------------------------
@mock.patch("main.storage.Client")
def test_upload_to_gcs(mock_storage_client):
    mock_blob = mock.Mock()
    mock_bucket = mock.Mock()
    mock_bucket.blob.return_value = mock_blob

    mock_client_instance = mock.Mock()
    mock_client_instance.bucket.return_value = mock_bucket
    mock_storage_client.return_value = mock_client_instance

    upload_to_gcs("my-bucket", "my-file.db", "/tmp/fake.db")

    mock_storage_client.assert_called_once()
    mock_bucket.blob.assert_called_with("my-file.db")
    mock_blob.upload_from_filename.assert_called_with("/tmp/fake.db")
