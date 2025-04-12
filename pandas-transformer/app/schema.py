from google.cloud.bigquery import SchemaField

INGESTION_SCHEMA = [
    "name",
    "original_price",
    "discount_price",
    "size",
    "category",
    "date",
]

PD_MERC_SCHEMA = {
    "date": "datetime64[ns]",
    "dedup_id": "int8",
    "name": "string",
    "size": "string",
    "category": "category",
    "subcategory": "category",
    "original_price": "float32",
    "prev_original_price": "float32",
    "discount_price": "float32",
    "is_fake_discount": "boolean",
    "inflation_percent": "float32",
    "inflation_abs": "float32",
}

BQ_MERC_SCHEMA = [
    SchemaField("date", "TIMESTAMP"),
    SchemaField("dedup_id", "INTEGER"),
    SchemaField("name", "STRING"),
    SchemaField("size", "STRING"),
    SchemaField("category", "STRING"),
    SchemaField("subcategory", "STRING"),
    SchemaField("original_price", "FLOAT"),
    SchemaField("prev_original_price", "FLOAT"),
    SchemaField("discount_price", "FLOAT"),
    SchemaField("is_fake_discount", "BOOLEAN"),
    SchemaField("inflation_percent", "FLOAT"),
    SchemaField("inflation_abs", "FLOAT"),
]
