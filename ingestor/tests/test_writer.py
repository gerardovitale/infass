from io import BytesIO
from unittest.mock import MagicMock
from unittest.mock import patch

import pandas as pd
import pytest
from writer import write_data
from writer import write_pandas_to_bucket_as_parquet


@pytest.fixture
def sample_data_gen():
    def _gen():
        yield pd.DataFrame({"col1": ["a", "b"], "col2": [1, 2]})
        yield pd.DataFrame({"col1": ["c"], "col2": [3]})

    return _gen


@pytest.fixture
def mock_gcs_client():
    with patch("writer.GCSClientSingleton") as mock_singleton:
        mock_client = MagicMock()
        mock_singleton.get_client.return_value = mock_client
        mock_bucket = MagicMock()
        mock_bucket.name = "test-bucket"
        mock_client.get_bucket.return_value = mock_bucket
        yield mock_client, mock_bucket


# ----------------------------------------------------------------------------------------------------------------------
# Test: write_data routing
# ----------------------------------------------------------------------------------------------------------------------


@patch("writer.write_pandas_to_local_csv")
def test_write_data_routes_to_local_csv_in_test_mode(mock_local_csv, sample_data_gen):
    gen = sample_data_gen()
    write_data(gen, "bucket", "prefix", test_mode=True)
    mock_local_csv.assert_called_once_with(gen, "prefix")


@patch("writer.write_pandas_to_bucket_as_parquet")
def test_write_data_routes_to_parquet_in_prod_mode(mock_parquet, sample_data_gen):
    gen = sample_data_gen()
    write_data(gen, "bucket", "prefix", test_mode=False)
    mock_parquet.assert_called_once_with(gen, "bucket", "prefix")


# ----------------------------------------------------------------------------------------------------------------------
# Test: write_pandas_to_bucket_as_parquet
# ----------------------------------------------------------------------------------------------------------------------


@patch("writer.datetime")
def test_write_parquet_uploads_single_file(mock_datetime, mock_gcs_client, sample_data_gen):
    mock_datetime.now.return_value.date.return_value.isoformat.return_value = "2024-01-15"
    mock_client, mock_bucket = mock_gcs_client

    write_pandas_to_bucket_as_parquet(sample_data_gen(), "test-bucket", "merc")

    mock_bucket.blob.assert_called_once_with("merc/2024-01-15.parquet")
    mock_bucket.blob.return_value.upload_from_file.assert_called_once()


@patch("writer.datetime")
def test_write_parquet_uploads_valid_parquet_content(mock_datetime, mock_gcs_client, sample_data_gen):
    mock_datetime.now.return_value.date.return_value.isoformat.return_value = "2024-01-15"
    mock_client, mock_bucket = mock_gcs_client

    uploaded_buffers = []
    blob_mock = mock_bucket.blob.return_value

    def capture_upload(buffer, content_type=None):
        uploaded_buffers.append(BytesIO(buffer.read()))

    blob_mock.upload_from_file.side_effect = capture_upload

    write_pandas_to_bucket_as_parquet(sample_data_gen(), "test-bucket", "merc")

    assert len(uploaded_buffers) == 1
    df = pd.read_parquet(uploaded_buffers[0])
    expected = pd.DataFrame({"col1": ["a", "b", "c"], "col2": [1, 2, 3]})
    pd.testing.assert_frame_equal(df, expected)


@patch("writer.datetime")
def test_write_parquet_skips_empty_chunks(mock_datetime, mock_gcs_client):
    mock_datetime.now.return_value.date.return_value.isoformat.return_value = "2024-01-15"
    mock_client, mock_bucket = mock_gcs_client

    def gen_with_empty():
        yield pd.DataFrame({"col1": ["a"]})
        yield pd.DataFrame()
        yield pd.DataFrame({"col1": ["b"]})

    write_pandas_to_bucket_as_parquet(gen_with_empty(), "test-bucket", "merc")

    mock_bucket.blob.assert_called_once_with("merc/2024-01-15.parquet")


@patch("writer.datetime")
def test_write_parquet_handles_all_empty_chunks(mock_datetime, mock_gcs_client):
    mock_datetime.now.return_value.date.return_value.isoformat.return_value = "2024-01-15"
    mock_client, mock_bucket = mock_gcs_client

    def gen_all_empty():
        yield pd.DataFrame()
        yield pd.DataFrame()

    write_pandas_to_bucket_as_parquet(gen_all_empty(), "test-bucket", "merc")

    mock_bucket.blob.assert_not_called()


@patch("writer.datetime")
def test_write_parquet_handles_empty_generator(mock_datetime, mock_gcs_client):
    mock_datetime.now.return_value.date.return_value.isoformat.return_value = "2024-01-15"
    mock_client, mock_bucket = mock_gcs_client

    def empty_gen():
        return
        yield

    write_pandas_to_bucket_as_parquet(empty_gen(), "test-bucket", "merc")

    mock_bucket.blob.assert_not_called()


@patch("writer.datetime")
def test_write_parquet_handles_null_typed_column_in_subsequent_chunk(mock_datetime, mock_gcs_client):
    mock_datetime.now.return_value.date.return_value.isoformat.return_value = "2024-01-15"
    mock_client, mock_bucket = mock_gcs_client
    uploaded_buffers = []
    blob_mock = mock_bucket.blob.return_value

    def capture_upload(buffer, content_type=None):
        uploaded_buffers.append(BytesIO(buffer.read()))

    blob_mock.upload_from_file.side_effect = capture_upload

    def gen_with_null_chunk():
        yield pd.DataFrame({"name": ["Product A"], "discount_price": ["1,53 €"]})
        yield pd.DataFrame({"name": ["Product B"], "discount_price": [None]})

    write_pandas_to_bucket_as_parquet(gen_with_null_chunk(), "test-bucket", "merc")

    assert len(uploaded_buffers) == 1
    df = pd.read_parquet(uploaded_buffers[0])
    assert list(df["name"]) == ["Product A", "Product B"]
    assert df["discount_price"][0] == "1,53 €"
    assert pd.isna(df["discount_price"][1])
