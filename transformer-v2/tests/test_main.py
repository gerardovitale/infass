from unittest.mock import MagicMock

import pandas as pd
from main import get_pipeline_config
from main import parse_args
from main import run_transformer
from schemas import BQ_SCHEMA
from transformers import CarrTransformer


def test_parse_args(monkeypatch):
    test_args = ["prog", "--gcs-source-bucket", "bucket", "--product", "merc", "--bq-destination-table", "table"]
    monkeypatch.setattr("sys.argv", test_args)
    args = parse_args()
    assert args.gcs_source_bucket == "bucket"
    assert args.product == "merc"
    assert args.bq_destination_table == "table"


def test_parse_args_with_carr_product(monkeypatch):
    test_args = ["prog", "--gcs-source-bucket", "bucket", "--product", "carr", "--bq-destination-table", "table"]
    monkeypatch.setattr("sys.argv", test_args)
    args = parse_args()
    assert args.product == "carr"


def test_get_pipeline_config_contains_carr_transformer():
    config = get_pipeline_config()
    assert config["carr"]["transformer"] is CarrTransformer
    assert config["carr"]["write_config"]["schema"] == BQ_SCHEMA


def test_run_transformer_skips_pipeline_on_empty_source_data():
    data_source = MagicMock()
    data_source.fetch_data.return_value = pd.DataFrame()
    transformer = MagicMock()
    destination = MagicMock()
    txn_recorder = MagicMock()
    txn_recorder.get_last_txn_if_exists.return_value = None

    run_transformer(
        data_source=data_source, transformer=transformer, destination=destination, txn_recorder=txn_recorder
    )

    transformer.transform.assert_not_called()
    destination.write_data.assert_not_called()
    txn_recorder.record.assert_not_called()


def test_run_transformer_skips_write_when_transform_result_is_empty():
    input_df = pd.DataFrame({"date": ["2026-02-21"], "original_price": ["1,00 €"], "discount_price": [None]})
    data_source = MagicMock()
    data_source.fetch_data.return_value = input_df
    transformer = MagicMock()
    transformer.transform.return_value = pd.DataFrame()
    destination = MagicMock()
    txn_recorder = MagicMock()
    txn_recorder.get_last_txn_if_exists.return_value = None

    run_transformer(
        data_source=data_source, transformer=transformer, destination=destination, txn_recorder=txn_recorder
    )

    transformer.transform.assert_called_once_with(input_df)
    destination.write_data.assert_not_called()
    txn_recorder.record.assert_not_called()
