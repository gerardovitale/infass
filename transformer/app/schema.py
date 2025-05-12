from google.cloud.bigquery import SchemaField

INGESTION_SCHEMA = [
    "date",
    "name",
    "size",
    "category",
    "original_price",
    "discount_price",
]

PD_MERC_SCHEMA = {
    "date": "datetime64[ns]",
    "dedup_id": "int8",
    "name": "string",
    "size": "string",
    "category": "category",
    "subcategory": "category",
    "price": "float32",
    "prev_price": "float32",
    "price_ma_7": "float32",
    "price_ma_15": "float32",
    "price_ma_30": "float32",
    "original_price": "float32",
    "prev_original_price": "float32",
    "discount_price": "float32",
    "price_var_abs": "float32",
    "price_var_percent": "float32",
    "original_price_var_abs": "float32",
    "original_price_var_percent": "float32",
    "is_fake_discount": "boolean",
}

BQ_MERC_SCHEMA = [
    SchemaField("date", "DATE"),
    SchemaField("dedup_id", "INTEGER"),
    SchemaField("name", "STRING"),
    SchemaField("size", "STRING"),
    SchemaField("category", "STRING"),
    SchemaField("subcategory", "STRING"),
    SchemaField("price", "NUMERIC"),
    SchemaField("prev_price", "NUMERIC"),
    SchemaField("price_ma_7", "NUMERIC"),
    SchemaField("price_ma_15", "NUMERIC"),
    SchemaField("price_ma_30", "NUMERIC"),
    SchemaField("original_price", "NUMERIC"),
    SchemaField("prev_original_price", "NUMERIC"),
    SchemaField("discount_price", "NUMERIC"),
    SchemaField("price_var_abs", "NUMERIC"),
    SchemaField("price_var_percent", "NUMERIC"),
    SchemaField("original_price_var_abs", "NUMERIC"),
    SchemaField("original_price_var_percent", "NUMERIC"),
    SchemaField("is_fake_discount", "BOOLEAN"),
]
