from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from main import run_transformer
from sinks import Sink
from transformers import add_price_column
from transformers import CarrTransformer
from transformers import cast_price_columns_as_float32
from transformers import cats_date_column
from transformers import deduplicate_products_with_diff_prices_per_date
from transformers import ensure_columns
from transformers import OUTPUT_SCHEMA
from transformers import parse_price_per_unit
from transformers import round_price_columns
from transformers import split_category_subcategory
from transformers import standardize_string_columns
from txn_rec import TransactionRecorder

CARREFOUR_CSV_FILENAME = "carr_2026-02-21T21:04.csv"


def _resolve_carrefour_csv_path() -> Path:
    tests_dir = Path(__file__).resolve().parent
    repo_root_data_path = tests_dir.parents[1] / "data" / "ingestor_output" / CARREFOUR_CSV_FILENAME
    local_fixture_path = tests_dir / "fixtures" / "carr_2026-02-21T21:04_sample.csv"

    if repo_root_data_path.exists():
        return repo_root_data_path
    if local_fixture_path.exists():
        return local_fixture_path
    raise FileNotFoundError("Carrefour CSV fixture not found in repository data folder or test fixtures.")


def _load_carrefour_df(nrows: int | None = None) -> pd.DataFrame:
    return pd.read_csv(_resolve_carrefour_csv_path(), nrows=nrows)


class InMemorySource(Sink):
    def __init__(self, data: pd.DataFrame):
        self.data = data

    def fetch_data(self, last_txn_date: str | None = None) -> pd.DataFrame:
        return self.data.copy()

    def write_data(self, df: pd.DataFrame) -> None:
        raise NotImplementedError()


class InMemoryDestination(Sink):
    def __init__(self):
        self.written_df = None

    def fetch_data(self, last_txn_date: str | None = None) -> pd.DataFrame:
        raise NotImplementedError()

    def write_data(self, df: pd.DataFrame) -> None:
        self.written_df = df.copy()


class InMemoryTxnRecorder(TransactionRecorder):
    def __init__(self):
        self.records = []

    def record(self, min_date: str | None, max_date: str | None) -> None:
        self.records.append((min_date, max_date))

    def get_last_txn_if_exists(self):
        return None


def test_carr_transformer_unit_with_carrefour_csv_sample():
    raw_df = _load_carrefour_df(nrows=250)

    actual = CarrTransformer().transform(raw_df)

    assert list(actual.columns) == list(OUTPUT_SCHEMA.keys())
    assert len(actual) == len(raw_df)
    assert str(actual["date"].dtype) == "datetime64[ns]"
    assert str(actual["dedup_id"].dtype) == "int8"
    assert str(actual["price"].dtype) == "float32"
    assert str(actual["category"].dtype) == "category"
    assert str(actual["subcategory"].dtype) == "category"
    assert actual["price"].notna().all()
    assert not actual["name"].str.contains(r"[A-Z]", na=False).any()


def test_carr_transformer_integration_pipeline_with_carrefour_csv():
    raw_df = _load_carrefour_df()
    source = InMemorySource(raw_df)
    destination = InMemoryDestination()
    txn_recorder = InMemoryTxnRecorder()

    run_transformer(
        data_source=source,
        transformer=CarrTransformer(),
        destination=destination,
        txn_recorder=txn_recorder,
    )

    assert destination.written_df is not None
    assert list(destination.written_df.columns) == list(OUTPUT_SCHEMA.keys())
    assert len(destination.written_df) == len(raw_df)
    assert txn_recorder.records == [("2026-02-21", "2026-02-21")]


def test_split_category_subcategory_handles_inputs_without_separator():
    df = pd.DataFrame({"category": ["frescos", "bebidas", "limpieza"]})

    actual = split_category_subcategory(df)

    assert list(actual["category"]) == ["frescos", "bebidas", "limpieza"]
    assert list(actual["subcategory"]) == ["frescos", "bebidas", "limpieza"]


def test_split_category_subcategory_handles_missing_category_column():
    df = pd.DataFrame({"name": ["a", "b"]})

    actual = split_category_subcategory(df)

    assert "category" in actual.columns
    assert "subcategory" in actual.columns
    assert actual["category"].isna().all()
    assert actual["subcategory"].isna().all()


def test_carr_transformer_handles_categories_without_separator():
    raw_df = pd.DataFrame(
        {
            "date": ["2026-02-21", "2026-02-21"],
            "name": ["Producto A", "Producto B"],
            "category": ["Frescos", "Bebidas"],
            "image_url": ["https://example.com/a.jpg", "https://example.com/b.jpg"],
            "original_price": ["1,20 €", "2,30 €"],
            "discount_price": [None, "1,99 €"],
        }
    )

    actual = CarrTransformer().transform(raw_df)

    assert list(actual["category"].astype("string")) == ["frescos", "bebidas"]
    assert list(actual["subcategory"].astype("string")) == ["frescos", "bebidas"]
    assert actual["price"].notna().all()


def test_carr_transformer_handles_missing_category_column():
    raw_df = pd.DataFrame(
        {
            "date": ["2026-02-21", "2026-02-21"],
            "name": ["Producto A", "Producto B"],
            "image_url": ["https://example.com/a.jpg", "https://example.com/b.jpg"],
            "original_price": ["1,20 €", "2,30 €"],
            "discount_price": [None, "1,99 €"],
        }
    )

    actual = CarrTransformer().transform(raw_df)

    assert "category" in actual.columns
    assert "subcategory" in actual.columns
    assert actual["category"].isna().all()
    assert actual["subcategory"].isna().all()


def test_carr_transformer_deduplicates_when_size_is_missing():
    raw_df = pd.DataFrame(
        {
            "date": ["2026-02-21", "2026-02-21"],
            "name": ["Producto A", "Producto A"],
            "category": ["Frescos > Carne", "Frescos > Carne"],
            "original_price": ["1,20 €", "1,30 €"],
        }
    )

    actual = CarrTransformer().transform(raw_df)

    assert list(actual["dedup_id"]) == [1, 2]


# ---------------------------------------------------------------------------
# Unit tests for individual transformation functions (data correctness)
# ---------------------------------------------------------------------------


def test_cats_date_column_produces_exact_timestamp():
    df = pd.DataFrame({"date": ["2026-02-21", "2026-01-15"]})

    actual = cats_date_column(df)

    assert actual["date"].iloc[0] == pd.Timestamp("2026-02-21")
    assert actual["date"].iloc[1] == pd.Timestamp("2026-01-15")
    assert actual["date"].dtype == "datetime64[ns]"


def test_cast_price_columns_as_float32_with_concrete_values():
    df = pd.DataFrame(
        {
            "original_price": ["1,70 €", "2,30 €", "1 €"],
            "discount_price": ["1,50 €", None, "0,99 €"],
        }
    )

    actual = cast_price_columns_as_float32(df)

    assert actual["original_price"].iloc[0] == pytest.approx(1.70, abs=1e-2)
    assert actual["original_price"].iloc[1] == pytest.approx(2.30, abs=1e-2)
    assert actual["original_price"].iloc[2] == pytest.approx(1.00, abs=1e-2)
    assert actual["discount_price"].iloc[0] == pytest.approx(1.50, abs=1e-2)
    assert pd.isna(actual["discount_price"].iloc[1])
    assert actual["discount_price"].iloc[2] == pytest.approx(0.99, abs=1e-2)
    assert actual["original_price"].dtype == "float32"
    assert actual["discount_price"].dtype == "float32"


def test_split_category_subcategory_with_concrete_split():
    df = pd.DataFrame({"category": ["Frescos > Ofertas", "Bebidas > Agua"]})

    actual = split_category_subcategory(df)

    assert list(actual["category"]) == ["Frescos", "Bebidas"]
    assert list(actual["subcategory"]) == ["Ofertas", "Agua"]


def test_standardize_string_columns_removes_accents_and_lowercases():
    df = pd.DataFrame(
        {
            "name": ["Plátano de Canarias", "Jamón Ibérico"],
            "size": [pd.NA, pd.NA],
            "category": ["Frescos", "Embutidos"],
            "subcategory": ["Frutas", "Jamón"],
        }
    )

    actual = standardize_string_columns(df)

    assert actual["name"].iloc[0] == "platano de canarias"
    assert actual["name"].iloc[1] == "jamon iberico"
    assert actual["category"].astype("string").iloc[0] == "frescos"
    assert actual["subcategory"].astype("string").iloc[1] == "jamon"


def test_deduplicate_products_assigns_correct_dedup_ids():
    df = pd.DataFrame(
        {
            "date": [pd.Timestamp("2026-02-21")] * 3,
            "name": ["producto a", "producto a", "producto b"],
            "size": [pd.NA, pd.NA, pd.NA],
            "category": pd.Categorical(["frescos", "frescos", "bebidas"]),
            "subcategory": pd.Categorical(["carne", "carne", "agua"]),
        }
    )

    actual = deduplicate_products_with_diff_prices_per_date(df)

    assert list(actual["dedup_id"]) == [1, 2, 1]


def test_add_price_column_priority_logic():
    df = pd.DataFrame(
        {
            "discount_price": pd.array([1.50, float("nan"), float("nan")], dtype="float32"),
            "original_price": pd.array([2.00, 2.00, 3.50], dtype="float32"),
        }
    )

    actual = add_price_column(df)

    assert actual["price"].iloc[0] == pytest.approx(1.50, abs=1e-2)
    assert actual["price"].iloc[1] == pytest.approx(2.00, abs=1e-2)
    assert actual["price"].iloc[2] == pytest.approx(3.50, abs=1e-2)


def test_round_price_columns_rounds_to_two_decimals():
    df = pd.DataFrame(
        {
            "original_price": pd.array([1.705, 2.999], dtype="float32"),
            "discount_price": pd.array([0.555, 1.004], dtype="float32"),
            "price": pd.array([1.705, 2.999], dtype="float32"),
            "price_per_unit": pd.array([1.705, 2.999], dtype="float32"),
        }
    )

    actual = round_price_columns(df)

    assert actual["original_price"].iloc[0] == pytest.approx(1.70, abs=1e-2)
    assert actual["original_price"].iloc[1] == pytest.approx(3.00, abs=1e-2)
    assert actual["discount_price"].iloc[0] == pytest.approx(0.56, abs=1e-2)
    assert actual["discount_price"].iloc[1] == pytest.approx(1.00, abs=1e-2)


def test_ensure_columns_adds_missing_column_with_default():
    df = pd.DataFrame({"name": ["a", "b"]})

    actual = ensure_columns(df, {"size": pd.NA})

    assert "size" in actual.columns
    assert actual["size"].isna().all()
    assert "name" in actual.columns


def test_ensure_columns_does_not_overwrite_existing_column():
    df = pd.DataFrame({"name": ["a", "b"], "size": ["100g", "200g"]})

    actual = ensure_columns(df, {"size": pd.NA})

    assert list(actual["size"]) == ["100g", "200g"]


# ---------------------------------------------------------------------------
# End-to-end data correctness test using fixture CSV
# ---------------------------------------------------------------------------


def test_carr_transformer_end_to_end_data_correctness_with_fixture():
    raw_df = _load_carrefour_df(nrows=15)

    actual = CarrTransformer().transform(raw_df)

    # Row 0: Plátano - only original_price, no discount
    row0 = actual.iloc[0]
    assert row0["name"] == "platano de canarias igp carrefour 900g aprox"
    assert row0["original_price"] == pytest.approx(1.70, abs=1e-2)
    assert pd.isna(row0["discount_price"])
    assert row0["price"] == pytest.approx(1.70, abs=1e-2)
    assert row0["date"] == pd.Timestamp("2026-02-21")
    assert row0["category"] == "frescos"
    assert row0["subcategory"] == "ofertas en productos frescos"
    assert row0["price_per_unit"] == pytest.approx(1.89, abs=1e-2)
    assert row0["unit"] == "kg"

    # Row 5 (index 5): Tomate ensalada - has both original and discount price
    row5 = actual.iloc[5]
    assert row5["name"] == "tomate ensalada carrefour 1 kg aprox"
    assert row5["original_price"] == pytest.approx(1.79, abs=1e-2)
    assert row5["discount_price"] == pytest.approx(1.69, abs=1e-2)
    assert row5["price"] == pytest.approx(1.69, abs=1e-2)
    assert row5["price_per_unit"] == pytest.approx(1.69, abs=1e-2)
    assert row5["unit"] == "kg"

    # Row 7 (index 7): Puerro - unit is "ud"
    row7 = actual.iloc[7]
    assert row7["price_per_unit"] == pytest.approx(1.85, abs=1e-2)
    assert row7["unit"] == "ud"

    # Row 13 (index 13): Tomate rosa - has both prices
    row13 = actual.iloc[13]
    assert row13["name"] == "tomate rosa carrefour 500 g aprox"
    assert row13["original_price"] == pytest.approx(1.67, abs=1e-2)
    assert row13["discount_price"] == pytest.approx(1.29, abs=1e-2)
    assert row13["price"] == pytest.approx(1.29, abs=1e-2)

    # All rows should have dedup_id=1 (no duplicates in first 15 rows)
    assert (actual["dedup_id"] == 1).all()

    # All dates should be 2026-02-21
    assert (actual["date"] == pd.Timestamp("2026-02-21")).all()


# ---------------------------------------------------------------------------
# Integration pipeline correctness test with crafted data
# ---------------------------------------------------------------------------


def test_integration_pipeline_correctness_with_crafted_data():
    raw_df = pd.DataFrame(
        {
            "date": ["2026-02-21", "2026-02-21", "2026-02-22"],
            "name": ["Plátano de Canarias", "Tomate Ensalada", "Plátano de Canarias"],
            "category": ["Frescos > Frutas", "Frescos > Verduras", "Frescos > Frutas"],
            "image_url": ["https://example.com/a.jpg", "https://example.com/b.jpg", "https://example.com/c.jpg"],
            "original_price": ["1,70 €", "2,30 €", "1,80 €"],
            "discount_price": [None, "1,99 €", None],
        }
    )
    source = InMemorySource(raw_df)
    destination = InMemoryDestination()
    txn_recorder = InMemoryTxnRecorder()

    run_transformer(
        data_source=source,
        transformer=CarrTransformer(),
        destination=destination,
        txn_recorder=txn_recorder,
    )

    result = destination.written_df
    assert result is not None
    assert len(result) == 3
    assert txn_recorder.records == [("2026-02-21", "2026-02-22")]

    # Row 0: only original_price → price = original_price
    assert result["name"].iloc[0] == "platano de canarias"
    assert result["price"].iloc[0] == pytest.approx(1.70, abs=1e-2)
    assert pd.isna(result["discount_price"].iloc[0])
    assert result["category"].iloc[0] == "frescos"
    assert result["subcategory"].iloc[0] == "frutas"

    # Row 1: has discount → price = discount_price
    assert result["name"].iloc[1] == "tomate ensalada"
    assert result["price"].iloc[1] == pytest.approx(1.99, abs=1e-2)
    assert result["discount_price"].iloc[1] == pytest.approx(1.99, abs=1e-2)
    assert result["category"].iloc[1] == "frescos"
    assert result["subcategory"].iloc[1] == "verduras"

    # Row 2: different date
    assert result["date"].iloc[2] == pd.Timestamp("2026-02-22")
    assert result["dedup_id"].iloc[2] == 1

    # All rows should have null price_per_unit and unit (crafted data has none)
    assert result["price_per_unit"].isna().all()
    assert result["unit"].isna().all()


# ---------------------------------------------------------------------------
# Unit tests for parse_price_per_unit
# ---------------------------------------------------------------------------


def test_parse_price_per_unit_extracts_value_and_unit():
    df = pd.DataFrame({"price_per_unit": ["1,89 €/kg", "1,85 €/ud", "10,43 €/kg"]})

    actual = parse_price_per_unit(df)

    assert actual["price_per_unit"].iloc[0] == pytest.approx(1.89, abs=1e-2)
    assert actual["price_per_unit"].iloc[1] == pytest.approx(1.85, abs=1e-2)
    assert actual["price_per_unit"].iloc[2] == pytest.approx(10.43, abs=1e-2)
    assert list(actual["unit"]) == ["kg", "ud", "kg"]
    assert actual["price_per_unit"].dtype == "float32"


def test_parse_price_per_unit_handles_nulls():
    df = pd.DataFrame({"price_per_unit": [pd.NA, "2,25 €/kg", None]})

    actual = parse_price_per_unit(df)

    assert pd.isna(actual["price_per_unit"].iloc[0])
    assert actual["price_per_unit"].iloc[1] == pytest.approx(2.25, abs=1e-2)
    assert pd.isna(actual["price_per_unit"].iloc[2])
    assert pd.isna(actual["unit"].iloc[0])
    assert actual["unit"].iloc[1] == "kg"
    assert pd.isna(actual["unit"].iloc[2])
