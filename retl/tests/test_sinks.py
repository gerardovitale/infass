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
def test_sqlite_sink_write_data(tmp_path, sample_df):
    db_path = tmp_path / "test.db"
    table_name = "test_table"
    sink = SQLiteSink(db_path=str(db_path), table=table_name)

    with patch("app.sinks.sqlite3.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        with patch.object(sample_df, "to_sql") as mock_to_sql:
            sink.write_data(sample_df)
            mock_connect.assert_called_once_with(str(db_path))
            mock_to_sql.assert_called_once()
            mock_conn.close.assert_called_once()
