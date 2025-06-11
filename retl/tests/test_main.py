from unittest.mock import MagicMock

from main import run_tasks


def test_run_tasks():
    mock_task1 = MagicMock()
    df1 = MagicMock()
    mock_task1.data_source.fetch_data.return_value = df1

    mock_task2 = MagicMock()
    df2 = MagicMock()
    mock_task2.data_source.fetch_data.return_value = df2

    run_tasks([mock_task1, mock_task2])

    mock_task1.data_source.fetch_data.assert_called_once_with()
    mock_task1.destination.write_data.assert_called_once_with(df1)
    mock_task2.data_source.fetch_data.assert_called_once_with()
    mock_task2.destination.write_data.assert_called_once_with(df2)
