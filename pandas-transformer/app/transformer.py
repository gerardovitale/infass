from __future__ import annotations

import logging
import re
import unicodedata
from datetime import date

import pandas as pd

from schema import DELTA_SCHEMA

logger = logging.getLogger(__name__)


def transformer(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Starting data transformation with df shape {df.shape}")
    df = cats_date_column(df)
    df = cast_price_columns_as_double(df)
    df = split_category_subcategory(df)
    df = standardize_string_columns(df)
    df = map_old_categories(df)
    df = deduplicate_products_with_diff_prices_per_date(df)
    df = add_prev_original_price(df)
    df = add_inflation_columns(df)
    df = add_is_fake_discount(df)
    return df[DELTA_SCHEMA]


def cats_date_column(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Casting date column")
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d").dt.date
    return df


def cast_price_columns_as_double(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Casting price columns as double")
    df["original_price"] = df["original_price"].str.replace("€", "").str.replace(",", ".").astype(float)
    df["discount_price"] = df["discount_price"].str.replace("€", "").str.replace(",", ".").astype(float)
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

    logger.info("Standardizing string columns")
    string_columns = ["name", "size", "category", "subcategory"]
    for column in string_columns:
        df[column] = df[column].str.lower().str.strip()
        df[column] = df[column].apply(strip_accents)
    return df


def map_old_categories(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Mapping old categories")
    recent_entries = df[df["date"] > date(2024, 11, 5)][["name", "size", "category", "subcategory"]].drop_duplicates()
    merged_df = pd.merge(df, recent_entries, on=["name", "size"], how="left", suffixes=("", "_new"))
    merged_df["category"] = merged_df["category_new"]
    merged_df["subcategory"] = merged_df["subcategory_new"]
    return merged_df.drop(columns=["category_new", "subcategory_new"]).drop_duplicates()


def deduplicate_products_with_diff_prices_per_date(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Deduplicating products with different prices per date")
    df["dedup_id"] = df.groupby(["date", "name", "size", "category", "subcategory"]).cumcount() + 1
    return df


def add_prev_original_price(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Adding prev_original_price column")
    df = df.sort_values(by=["name", "size", "date"])
    df["prev_original_price"] = df.groupby(["name", "size"])["original_price"].shift(1)
    return df


def add_inflation_columns(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Adding inflation columns: inflation_percent, inflation_abs")
    df["inflation_percent"] = df["original_price"] / df["prev_original_price"] - 1
    df["inflation_abs"] = df["original_price"] - df["prev_original_price"]
    return df


def add_is_fake_discount(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Adding is_fake_discount column")
    df["is_fake_discount"] = None
    df.loc[df["discount_price"].notna(), "is_fake_discount"] = df["discount_price"] == df["prev_original_price"]
    return df


def create_size_pattern_column(df: pd.DataFrame) -> pd.DataFrame:
    def is_number(s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def generate_pattern(size: str):
        units = ["mg", "g", "kg", "ml", "cl", "l"]
        containers = ["lata", "latas", "botella", "botellas", "garrafa", "brick", "botellin", "botellines", "caja",
                      "sobres", "ud"]
        size = re.sub(r"[().]", "", size)  # Remove dots and parentheses
        size = size.replace("x", "")  # Replace "x" with space
        size = re.sub(r'\d+,\d*', lambda x: x.group().replace(',', '.'), size)
        tokens = size.split()
        print(tokens)
        pattern = []
        for token in tokens:
            if token in containers:
                pattern.append("container")
            elif is_number(token):
                pattern.append("x")
            elif token in units:
                pattern.append("unit")
            else:
                pattern.append(f"<{token}>")
        return " ".join(pattern)

    df["size_pattern"] = df["size"].apply(generate_pattern)
    return df


def standardize_size_columns(df: pd.DataFrame) -> pd.DataFrame:
    def parse_size(size: str):
        # Match patterns for multiple containers (e.g., "6 botellas x 1,5 l")
        multiple_match = re.match(r"(\d+)\s*([\w.]+)s?\s*x\s*(\d+[,.]?\d*)\s*([a-zA-Z]+)", size)
        if multiple_match:
            container_n = int(multiple_match.group(1))
            container = multiple_match.group(2).strip()
            quantity = float(multiple_match.group(3).replace(",", "."))
            unit = multiple_match.group(4).strip()
            return container_n, container, quantity, unit

        # Match patterns for single containers (e.g., "botella 1,5 l")
        single_match = re.match(r"([\w.]+)\s*(\d+[,.]?\d*)\s*([a-zA-Z]+)", size)
        if single_match:
            container_n = 1  # Default for single containers
            container = single_match.group(1).strip()
            quantity = float(single_match.group(2).replace(",", "."))
            unit = single_match.group(3).strip()
            return container_n, container, quantity, unit

        return None, None, None, None

    df[["container_n", "container", "quantity", "unit"]] = df["size"].apply(lambda x: pd.Series(parse_size(x)))
    return df[["container_n", "container", "quantity", "unit"]]
