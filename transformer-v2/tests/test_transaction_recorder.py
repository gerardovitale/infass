import sqlite3

import pytest
from transaction_recorder import TxnRecSQLite


@pytest.fixture
def sqlite_db_path(tmp_path):
    db_path = tmp_path / "test_db.sqlite"
    conn = sqlite3.connect(db_path)
    conn.close()
    return str(db_path)


def test_transaction_recorder_init_with_valid_db_path(sqlite_db_path):
    product = "merc"
    data_source = "source_table"
    destination = "destination_table"

    recorder = TxnRecSQLite(sqlite_db_path, product, data_source, destination)

    assert recorder.db_path == sqlite_db_path
    assert recorder.product == product
    assert recorder.data_source == data_source
    assert recorder.destination == destination


def test_transaction_recorder_init_with_invalid_db_path():
    invalid_path = "/tmp/nonexistent_db.sqlite"
    product = "merc"
    data_source = "source_table"
    destination = "destination_table"

    with pytest.raises(Exception):
        TxnRecSQLite(invalid_path, product, data_source, destination)
