from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest
from sinks import Storage


class MockStorageBlob:
    def __init__(self, name, content):
        self.name = name
        self.content = content

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, MockStorageBlob):
            return NotImplemented
        return self.name == value.name and self.content == value.content


@pytest.fixture
def mock_storage_client_obj():
    with patch("sinks.StorageClient") as MockStorageClient:
        yield MockStorageClient.return_value


@pytest.fixture
def test_storage(mock_storage_client_obj):
    return Storage(bucket_name="test_bucket", prefix="test_prefix")


# ----------------------------------------------------------------------------------------------------------------------
# Test: Storage init
# ----------------------------------------------------------------------------------------------------------------------


def test_storage_init():
    with patch("sinks.StorageClient") as mock_storage_client_cls:
        test_bucket_name = "test_bucket_name"
        test_prefix = "test_prefix"

        storage = Storage(test_bucket_name, test_prefix)

        assert storage.bucket_name == test_bucket_name
        assert storage.prefix == test_prefix
        mock_storage_client_cls.assert_called_once()


# ----------------------------------------------------------------------------------------------------------------------
# Test: Storage fetch_data
# ----------------------------------------------------------------------------------------------------------------------


@patch.object(Storage, "filter_by_date")
@patch.object(Storage, "build_dataframe")
def test_fetch_data_when_last_txn_date_is_none(
    mock_build_dataframe, mock_filter_by_date, test_storage, mock_storage_client_obj
):
    mock_storage_client_obj.list_blobs.return_value = (
        MockStorageBlob(f"2025-01-0{i}.csv", "mock_content") for i in range(1, 4)
    )
    mock_build_dataframe.return_value = pd.DataFrame({"col1": ["value1", "value1", "value2", "value3", "value3"]})

    actual = test_storage.fetch_data()

    mock_filter_by_date.assert_not_called()
    mock_build_dataframe.assert_called_once_with(mock_storage_client_obj.list_blobs.return_value)
    pd.testing.assert_frame_equal(actual, mock_build_dataframe.return_value)


@patch.object(Storage, "filter_by_date")
@patch.object(Storage, "build_dataframe")
def test_fetch_data_when_last_txn_date_is_a_str_date(
    mock_build_dataframe, mock_filter_by_date, test_storage, mock_storage_client_obj
):
    test_last_txn_date = "2025-01-01"
    mock_storage_client_obj.list_blobs.return_value = (
        MockStorageBlob(f"2025-01-0{i}.csv", "mock_content") for i in range(1, 4)
    )
    mock_filter_by_date.return_value = (MockStorageBlob(f"2025-01-0{i}.csv", "mock_content") for i in range(2, 4))
    mock_build_dataframe.return_value = pd.DataFrame({"col1": ["value2", "value3", "value3"]})

    actual = test_storage.fetch_data(test_last_txn_date)

    mock_filter_by_date.assert_called_once_with(mock_storage_client_obj.list_blobs.return_value, test_last_txn_date)
    pd.testing.assert_frame_equal(actual, mock_build_dataframe.return_value)


# ----------------------------------------------------------------------------------------------------------------------
# Test: Storage build_dataframe
# ----------------------------------------------------------------------------------------------------------------------


@pytest.fixture
def mock_storage_read_blob():
    def mock_read_blob(blob_name):
        if blob_name == "name1":
            return pd.DataFrame({"col1": ["value1", "value1"]})
        elif blob_name == "name2":
            return pd.DataFrame({"col1": ["value2"]})
        elif blob_name == "name3":
            return pd.DataFrame({"col1": ["value3", "value3"]})
        else:
            return pd.DataFrame()

    with patch.object(Storage, "read_blob", side_effect=mock_read_blob) as mock_read_blob:
        yield mock_read_blob


def test_build_dataframe(test_storage, mock_storage_read_blob):
    test_storage_blobs = [
        MockStorageBlob("name1", "content1"),
        MockStorageBlob("name2", "content2"),
        MockStorageBlob("name3", "content3"),
    ]
    expected = pd.DataFrame({"col1": ["value1", "value1", "value2", "value3", "value3"]})

    actual = test_storage.build_dataframe(test_storage_blobs)

    mock_storage_read_blob.assert_called()
    pd.testing.assert_frame_equal(actual, expected)


def test_build_dataframe_when_read_blod_return_empty_dfs(test_storage, mock_storage_read_blob):
    test_storage_blobs = [
        # MockStorageBlob names not contained in mock_storage_read_blob
        MockStorageBlob("name4", "content1"),
        MockStorageBlob("name5", "content2"),
        MockStorageBlob("name6", "content3"),
    ]

    actual = test_storage.build_dataframe(test_storage_blobs)

    mock_storage_read_blob.assert_called()
    assert actual.empty


def test_build_dataframe_when_storage_blobs_iter_is_empty(test_storage, mock_storage_read_blob):
    test_storage_blobs = []
    actual = test_storage.build_dataframe(test_storage_blobs)
    mock_storage_read_blob.assert_not_called()
    assert actual.empty


# ----------------------------------------------------------------------------------------------------------------------
# Test: Storage filter_by_date
# ----------------------------------------------------------------------------------------------------------------------


def test_filter_by_date_basic(test_storage):
    files = [
        MockStorageBlob("data/2023-08-20-raw.csv", "mock_content"),
        MockStorageBlob("data/2023-08-21-raw.csv", "mock_content"),
        MockStorageBlob("data/2023-08-22-raw.csv", "mock_content"),
        MockStorageBlob("data/2023-08-19-raw.csv", "mock_content"),
        MockStorageBlob("data/invalid.csv", "mock_content"),
        MockStorageBlob("data/2022-02-29-raw.csv", "mock_content"),  # invalid date
        MockStorageBlob("data/2024-02-29-raw.csv", "mock_content"),  # leap year
    ]
    expected = [
        MockStorageBlob("data/2023-08-21-raw.csv", "mock_content"),
        MockStorageBlob("data/2023-08-22-raw.csv", "mock_content"),
        MockStorageBlob("data/2024-02-29-raw.csv", "mock_content"),
    ]
    result = test_storage.filter_by_date(files, "2023-08-20")
    assert list(result) == expected


def test_filter_by_date_no_valid_dates(test_storage):
    files = [
        MockStorageBlob("data/invalid.csv", "mock_content"),
        MockStorageBlob("data/2022-02-29-raw.csv", "mock_content"),
    ]
    result = test_storage.filter_by_date(files, "2023-08-20")
    assert list(result) == []


def test_filter_by_date_all_before(test_storage):
    files = [
        MockStorageBlob("data/2023-08-18-raw.csv", "mock_content"),
        MockStorageBlob("data/2023-08-19-raw.csv", "mock_content"),
    ]
    result = test_storage.filter_by_date(files, "2023-08-20")
    assert list(result) == []


def test_filter_by_date_edge_case(test_storage):
    files = [
        MockStorageBlob("data/2023-08-20-raw.csv", "mock_content"),
        MockStorageBlob("data/2023-08-21-raw.csv", "mock_content"),
    ]
    expected = [
        MockStorageBlob("data/2023-08-21-raw.csv", "mock_content"),
    ]
    result = test_storage.filter_by_date(files, "2023-08-20")
    assert list(result) == expected


# ----------------------------------------------------------------------------------------------------------------------
# Test: Storage extract_date
# ----------------------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename,expected",
    [
        ("data/2023-08-22-raw.csv", date(2023, 8, 22)),
        ("/tmp/2021-01-01.csv", date(2021, 1, 1)),
        ("prefix/2020-12-31_extra.csv", date(2020, 12, 31)),
        ("no-date-here.csv", None),
        ("2022-02-29.csv", None),  # invalid date
        ("2024-02-29.csv", date(2024, 2, 29)),  # leap year
        ("2023-13-01.csv", None),  # invalid month
        ("2023-00-01.csv", None),  # invalid month
        ("2023-12-32.csv", None),  # invalid day
    ],
)
def test_extract_date(filename, expected):
    result = Storage.extract_date(filename)
    assert result == expected
