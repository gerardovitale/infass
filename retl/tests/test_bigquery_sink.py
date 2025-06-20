from unittest.mock import MagicMock
from unittest.mock import Mock

import pandas as pd
import pytest
from bigquery_sink import BigQuerySink
from google.cloud import bigquery
from pandas.testing import assert_frame_equal
from sink import Transaction


@pytest.fixture
def sample_df():
    return pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})


# --------------------------
# Test: BigQuerySink
# --------------------------
def test_bigquery_sink_fetch_data(sample_df):
    mock_client = Mock(spec=bigquery.Client)
    test_params = {
        "project_id": "test_project_id",
        "dataset_id": "test_dataset_id",
        "table": "test_table",
        "client": mock_client,
    }

    mock_query_job = MagicMock()
    mock_query_job.to_dataframe.return_value = sample_df
    mock_client.query.return_value = mock_query_job

    sink = BigQuerySink(**test_params)
    df = sink.fetch_data()

    mock_client.query.assert_called_once()
    assert_frame_equal(df, sample_df)


def test_bigquery_sink_fetch_data_with_last_transaction(sample_df):
    mock_client = Mock(spec=bigquery.Client)
    test_params = {
        "project_id": "test_project_id",
        "dataset_id": "test_dataset_id",
        "table": "test_table",
        "client": mock_client,
    }
    test_last_transaction = Transaction(
        data_source_table="src",
        destination_table="test_table",
        occurred_at="2025-06-14T07:00:00",
        min_date="2025-06-07",
        max_date="2025-06-14",
    )

    mock_query_job = MagicMock()
    mock_query_job.to_dataframe.return_value = sample_df
    mock_client.query.return_value = mock_query_job

    expected_query = "SELECT * FROM `test_project_id.test_dataset_id.test_table` WHERE date >= '2025-06-14'"

    sink = BigQuerySink(**test_params)
    df = sink.fetch_data(test_last_transaction)

    mock_client.query.assert_called_once_with(expected_query)
    assert_frame_equal(df, sample_df)


def test_bigquery_sink_fetch_data_empty_result():
    mock_client = Mock(spec=bigquery.Client)
    test_params = {
        "project_id": "test_project_id",
        "dataset_id": "test_dataset_id",
        "table": "test_table",
        "client": mock_client,
    }

    mock_query_job = MagicMock()
    empty_df = pd.DataFrame()
    mock_query_job.to_dataframe.return_value = empty_df
    mock_client.query.return_value = mock_query_job

    sink = BigQuerySink(**test_params)
    df = sink.fetch_data()

    assert_frame_equal(df, empty_df)


def test_bigquery_sink_fetch_data_raises_exception():
    mock_client = Mock(spec=bigquery.Client)
    test_params = {
        "project_id": "test_project_id",
        "dataset_id": "test_dataset_id",
        "table": "test_table",
        "client": mock_client,
    }
    mock_client.query.side_effect = Exception("BigQuery error")
    sink = BigQuerySink(**test_params)
    with pytest.raises(Exception, match="BigQuery error"):
        sink.fetch_data()
