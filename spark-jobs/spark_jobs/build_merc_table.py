from __future__ import annotations

import logging
import sys
import unicodedata
from typing import Callable

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    broadcast,
    col,
    lag,
    lower,
    regexp_replace,
    row_number,
    split,
    trim,
    udf,
    when,
)
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger(__name__)

INGESTION_SCHEMA = StructType(
    [
        StructField("name", StringType()),
        StructField("original_price", StringType()),
        StructField("discount_price", StringType()),
        StructField("size", StringType()),
        StructField("category", StringType()),
        StructField("date", DateType()),
    ]
)
RESULTING_SCHEMA = StructType(
    [
        StructField("date", DateType()),
        StructField("dedup_id", IntegerType(), nullable=False),
        StructField("name", StringType()),
        StructField("size", StringType()),
        StructField("category", StringType()),
        StructField("subcategory", StringType()),
        StructField("original_price", DoubleType()),
        StructField("prev_original_price", DoubleType()),
        StructField("discount_price", DoubleType()),
        StructField("is_fake_discount", BooleanType()),
        StructField("inflation_percent", DoubleType()),
        StructField("inflation_abs", DoubleType()),
    ]
)


def build_table(
    data_source_bucket_uri: str,
    schema: StructType,
    transformation_func: Callable[[DataFrame], DataFrame] | None,
    bigquery_destination_table: str,
    write_mode: str = "overwrite",
) -> None:
    spark = SparkSession.builder.appName("Build InfAss Table").getOrCreate()
    logger.info("Building table based on data source: {0}".format(data_source_bucket_uri))
    df = spark.read.csv(data_source_bucket_uri, header=True, schema=schema)
    if transformation_func:
        logger.info("Applying transformation: {0}".format(transformation_func.__name__))
        df = transformation_func(df)
    logger.info("Writing delta table at {0}".format(bigquery_destination_table))
    (df.write.format("bigquery")
     .option("table", bigquery_destination_table)
     .option("writeMethod", "direct")
     .mode(write_mode).save()
     )
    spark.stop()


def process_raw_data(df: DataFrame) -> DataFrame:
    return (
        df.transform(cast_price_columns_as_double)
        .transform(split_category_subcategory)
        .transform(standardize_string_columns)
        .transform(map_old_categories)
        .transform(deduplicate_products_with_diff_prices_per_date)
        .transform(add_prev_original_price)
        .transform(add_inflation_columns)
        .transform(add_is_fake_discount)
        .select(RESULTING_SCHEMA.fieldNames())
    )


def cast_price_columns_as_double(df: DataFrame) -> DataFrame:
    def _cast_price_columns_as_double(column: str | Column) -> Column:
        column = regexp_replace(column, r"[€]", "")
        return regexp_replace(column, r",", ".").cast(DoubleType())

    return df.withColumn("original_price", _cast_price_columns_as_double("original_price")).withColumn(
        "discount_price", _cast_price_columns_as_double("discount_price")
    )


def split_category_subcategory(df: DataFrame) -> DataFrame:
    category_subcategory_sep = " > "
    return df.withColumn("subcategory", split(col("category"), category_subcategory_sep).getItem(1)).withColumn(
        "category", split(col("category"), category_subcategory_sep).getItem(0)
    )


def standardize_string_columns(df: DataFrame) -> DataFrame:
    @udf(returnType=StringType())
    def udf_strip_accents(string):
        return strip_accents(string)

    string_columns = ["name", "size", "category", "subcategory"]
    for column in string_columns:
        df = df.withColumn(column, clean_string(column)).withColumn(column, udf_strip_accents(column))
    return df


def map_old_categories(df: DataFrame) -> DataFrame:
    category_mapping_df = broadcast(
        df.filter(col("date") > "2024-11-05").select("name", "size", "category", "subcategory").distinct()
    )
    return (
        df.withColumnRenamed("category", "old_category")
        .withColumnRenamed("subcategory", "old_subcategory")
        .join(category_mapping_df, on=["name", "size"], how="left")
        .drop("old_category", "old_subcategory")
        .drop_duplicates()
        .select(df.columns)
    )


def deduplicate_products_with_diff_prices_per_date(df: DataFrame) -> DataFrame:
    dedup_window = Window.partitionBy("date", "name", "size", "category", "subcategory").orderBy(
        "date", "original_price"
    )
    return df.withColumn("dedup_id", row_number().over(dedup_window))


def add_prev_original_price(df: DataFrame) -> DataFrame:
    product_window = Window.partitionBy("name", "size", "dedup_id").orderBy("date")
    return df.withColumn("prev_original_price", lag("original_price").over(product_window))


def add_inflation_columns(df: DataFrame) -> DataFrame:
    return df.withColumn("inflation_percent", col("original_price") / col("prev_original_price") - 1).withColumn(
        "inflation_abs", col("original_price") - col("prev_original_price")
    )


def add_is_fake_discount(df: DataFrame) -> DataFrame:
    return df.withColumn("is_fake_discount", when(col("discount_price") == col("prev_original_price"), True))


def clean_string(column: Column | str) -> Column:
    return trim(lower(regexp_replace(column, " +", " ")))


def strip_accents(string: str) -> str:
    return "".join(char for char in unicodedata.normalize("NFD", string) if unicodedata.category(char) != "Mn")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        logger.error(f"Arguments has not been provided correctly: {sys.argv}")
        sys.exit(-1)

    params = {
        "data_source": sys.argv[1],                 # gs://bucket_name/
        "schema": INGESTION_SCHEMA,
        "transformation_func": process_raw_data,
        "bigquery_destination_table": sys.argv[2],  # project_id:dataset_name.table_name
    }
    build_table(**params)
