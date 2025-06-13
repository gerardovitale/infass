from unittest.mock import MagicMock
from unittest.mock import patch

import pandas as pd
import pytest
from app.sinks import BigQuerySink
from app.sinks import SQLiteSink


@pytest.fixture
def sample_df():
    return pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})


# --------------------------
# Test: BigQuerySink
# --------------------------
def test_bigquery_sink_fetch_data(sample_df):
    with patch("app.sinks.bigquery.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_query_job = MagicMock()
        mock_query_job.to_dataframe.return_value = sample_df
        mock_client.query.return_value = mock_query_job
        mock_client_cls.return_value = mock_client

        sink = BigQuerySink(project_id="proj", dataset_id="ds", table="tbl")
        df = sink.fetch_data()

        mock_client_cls.assert_called_once_with(project="proj")
        mock_client.query.assert_called_once()
        assert df.equals(sample_df)


# --------------------------
# Test: SQLiteSink
# --------------------------
@pytest.fixture
def mock_sqlite_connect():
    with patch("app.sinks.sqlite3.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        yield mock_connect, mock_conn


def test_sqlite_sink_write_data_when_index_columns_is_none(tmp_path, mock_sqlite_connect, sample_df):
    test_params = {
        "db_path": str(tmp_path / "test.db"),
        "table": "test_table",
    }
    mock_connect, mock_conn = mock_sqlite_connect

    sink = SQLiteSink(**test_params)

    with patch("pandas.DataFrame.to_sql") as mock_to_sql:
        sink.write_data(sample_df)
        mock_connect.assert_called_once_with(test_params["db_path"])
        mock_to_sql.assert_called_once_with(test_params["table"], mock_conn, if_exists="replace", index=False)
        mock_conn.close.assert_called_once()


def test_sqlite_sink_write_data_when_index_columns_are_specified(tmp_path, mock_sqlite_connect, sample_df):
    test_params = {
        "db_path": str(tmp_path / "test.db"),
        "table": "test_table",
        "index_columns": ["a", "b"],
    }
    mock_connect, mock_conn = mock_sqlite_connect

    sink = SQLiteSink(**test_params)

    with patch("pandas.DataFrame.to_sql") as mock_to_sql:
        sink.write_data(sample_df)
        mock_connect.assert_called_once_with(test_params["db_path"])
        mock_to_sql.assert_called_once_with(
            test_params["table"], mock_conn, if_exists="replace", index=True, index_label=test_params["index_columns"]
        )
        mock_conn.close.assert_called_once()
