from unittest.mock import MagicMock

import pandas as pd
from main import parse_args
from main import PIPELINE_DEFAULT_CONFIG
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


def test_pipeline_default_config_contains_carr_transformer():
    assert PIPELINE_DEFAULT_CONFIG["carr"]["transformer"] is CarrTransformer
    assert PIPELINE_DEFAULT_CONFIG["carr"]["write_config"]["schema"] == BQ_SCHEMA


def test_run_transformer_skips_pipeline_on_empty_source_data():
    data_source = MagicMock()
    data_source.fetch_data.return_value = pd.DataFrame()
    transformer = MagicMock()
    destination = MagicMock()
    txn_recorder = MagicMock()
    txn_recorder.get_last_txn_if_exists.return_value = None

    run_transformer(
        data_source=data_source,
        transformer=transformer,
        destination=destination,
        txn_recorder=txn_recorder,
        reprocess=False,
    )

    transformer.transform.assert_not_called()
    destination.write_data.assert_not_called()
    txn_recorder.record.assert_not_called()


def test_parse_args_with_reprocess_flag(monkeypatch):
    test_args = [
        "prog",
        "--gcs-source-bucket",
        "bucket",
        "--product",
        "merc",
        "--bq-destination-table",
        "table",
        "--reprocess",
    ]
    monkeypatch.setattr("sys.argv", test_args)
    args = parse_args()
    assert args.reprocess is True


def test_parse_args_without_reprocess_flag(monkeypatch):
    test_args = ["prog", "--gcs-source-bucket", "bucket", "--product", "merc", "--bq-destination-table", "table"]
    monkeypatch.setattr("sys.argv", test_args)
    args = parse_args()
    assert args.reprocess is False


def test_run_transformer_reprocess_mode_skips_txn_lookup():
    input_df = pd.DataFrame({"date": ["2026-02-21"], "original_price": ["1,00 €"], "discount_price": [None]})
    transformed_df = pd.DataFrame({"date": ["2026-02-21"], "original_price": [1.0], "discount_price": [None]})
    data_source = MagicMock()
    data_source.fetch_data.return_value = input_df
    transformer = MagicMock()
    transformer.transform.return_value = transformed_df
    destination = MagicMock()
    txn_recorder = MagicMock()

    run_transformer(
        data_source=data_source,
        transformer=transformer,
        destination=destination,
        txn_recorder=txn_recorder,
        reprocess=True,
    )

    txn_recorder.get_last_txn_if_exists.assert_not_called()
    data_source.fetch_data.assert_called_once_with(last_txn_date=None)
    destination.write_data.assert_called_once_with(transformed_df)


def test_run_transformer_reprocess_mode_skips_pipeline_on_empty_source_data():
    data_source = MagicMock()
    data_source.fetch_data.return_value = pd.DataFrame()
    transformer = MagicMock()
    destination = MagicMock()
    txn_recorder = MagicMock()

    run_transformer(
        data_source=data_source,
        transformer=transformer,
        destination=destination,
        txn_recorder=txn_recorder,
        reprocess=True,
    )

    txn_recorder.get_last_txn_if_exists.assert_not_called()
    data_source.fetch_data.assert_called_once_with(last_txn_date=None)
    transformer.transform.assert_not_called()
    destination.write_data.assert_not_called()
    txn_recorder.record.assert_not_called()


def test_run_transformer_normal_mode_uses_txn_lookup():
    input_df = pd.DataFrame({"date": ["2026-02-21"], "original_price": ["1,00 €"], "discount_price": [None]})
    transformed_df = pd.DataFrame({"date": ["2026-02-21"], "original_price": [1.0], "discount_price": [None]})
    last_txn = MagicMock()
    last_txn.max_date = "2026-02-20"
    data_source = MagicMock()
    data_source.fetch_data.return_value = input_df
    transformer = MagicMock()
    transformer.transform.return_value = transformed_df
    destination = MagicMock()
    txn_recorder = MagicMock()
    txn_recorder.get_last_txn_if_exists.return_value = last_txn

    run_transformer(
        data_source=data_source,
        transformer=transformer,
        destination=destination,
        txn_recorder=txn_recorder,
        reprocess=False,
    )

    txn_recorder.get_last_txn_if_exists.assert_called_once()
    data_source.fetch_data.assert_called_once_with(last_txn_date="2026-02-20")
    destination.write_data.assert_called_once_with(transformed_df)


def test_pipeline_default_config_not_mutated_after_reprocess():
    original_disposition = PIPELINE_DEFAULT_CONFIG["merc"]["write_config"]["write_disposition"]
    assert original_disposition == "WRITE_APPEND"

    write_config = {**PIPELINE_DEFAULT_CONFIG["merc"]["write_config"]}
    write_config["write_disposition"] = "WRITE_TRUNCATE"

    assert PIPELINE_DEFAULT_CONFIG["merc"]["write_config"]["write_disposition"] == "WRITE_APPEND"


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
        data_source=data_source,
        transformer=transformer,
        destination=destination,
        txn_recorder=txn_recorder,
        reprocess=False,
    )

    transformer.transform.assert_called_once_with(input_df)
    destination.write_data.assert_not_called()
    txn_recorder.record.assert_not_called()
