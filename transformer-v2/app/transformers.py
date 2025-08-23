import logging
import unicodedata
from abc import ABC
from abc import abstractmethod

import numpy as np
import pandas as pd

MERC_SCHEMA = {
    "date": "datetime64[ns]",
    "dedup_id": "int8",
    "name": "string",
    "size": "string",
    "category": "category",
    "subcategory": "category",
    "image_url": "object",
    "price": "float32",
    "price_ma_7": "float32",
    "price_ma_15": "float32",
    "price_ma_30": "float32",
    "original_price": "float32",
    "discount_price": "float32",
}


logger = logging.getLogger(__name__)


class Transformer(ABC):
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()


class MercTransformer(Transformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Executing MercTransformer")
        return transform_merc(df)


def transform_merc(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Starting data transformation with df shape {df.shape}")
    df = cats_date_column(df)
    df = cast_price_columns_as_float32(df)
    df = split_category_subcategory(df)
    df = map_old_categories(df)
    df = standardize_string_columns(df)
    df = deduplicate_products_with_diff_prices_per_date(df)
    df = add_price_column(df)
    df = round_price_columns(df)
    return df[MERC_SCHEMA.keys()]


def cats_date_column(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Casting date column")
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    return df


def cast_price_columns_as_float32(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Casting price columns as float32")
    price_columns = ["original_price", "discount_price"]
    for col in price_columns:
        df[col] = df[col].str.replace("€", "", regex=False).str.replace(",", ".", regex=False)
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")
    return df


def split_category_subcategory(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Splitting category and subcategory")
    category_subcategory_sep = " > "
    df[["category", "subcategory"]] = df["category"].str.split(category_subcategory_sep, expand=True, n=1)
    return df


def standardize_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    def strip_accents(string: str) -> str:
        if pd.isna(string):
            return string
        return "".join(char for char in unicodedata.normalize("NFD", string) if unicodedata.category(char) != "Mn")

    def cast_string_columns(_df: pd.DataFrame) -> pd.DataFrame:
        dtype_map = {
            "name": "string",
            "size": "string",
            "category": "category",
            "subcategory": "category",
        }
        return _df.astype(dtype_map)

    logger.info("Standardizing string columns")
    string_columns = ["name", "size", "category", "subcategory"]
    for column in string_columns:
        df[column] = df[column].astype("string").str.lower().str.strip().apply(strip_accents)
    return cast_string_columns(df)


def map_old_categories(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Mapping old categories")
    recent_entries = df[df["date"] > "2024-11-05"][["name", "size", "category", "subcategory"]].drop_duplicates()
    merged_df = pd.merge(df, recent_entries, on=["name", "size"], how="left", suffixes=("", "_new"))
    merged_df["category"] = merged_df["category_new"]
    merged_df["subcategory"] = merged_df["subcategory_new"]
    return merged_df.drop(columns=["category_new", "subcategory_new"]).drop_duplicates()


def deduplicate_products_with_diff_prices_per_date(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Deduplicating products with different prices per date")
    df["dedup_id"] = df.groupby(["date", "name", "size", "category", "subcategory"], observed=True).cumcount() + 1
    df["dedup_id"] = df["dedup_id"].fillna(1).astype("int8")
    return df


def add_price_column(df: pd.DataFrame) -> pd.DataFrame:
    df["price"] = np.where(df["discount_price"].notna(), df["discount_price"], df["original_price"])
    return df


def round_price_columns(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Rounding price columns to 2 decimal places")
    price_columns = ["original_price", "discount_price", "price"]
    df[price_columns] = df[price_columns].round(2)
    return df
