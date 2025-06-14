from google.cloud.bigquery import SchemaField

INGESTION_SCHEMA = [
    "date",
    "name",
    "size",
    "category",
    "original_price",
    "discount_price",
    "image_url",
]

PD_MERC_SCHEMA = {
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

BQ_MERC_SCHEMA = [
    SchemaField("date", "DATE"),
    SchemaField("dedup_id", "INTEGER"),
    SchemaField("name", "STRING"),
    SchemaField("size", "STRING"),
    SchemaField("category", "STRING"),
    SchemaField("subcategory", "STRING"),
    SchemaField("image_url", "STRING"),
    SchemaField("price", "FLOAT"),
    SchemaField("price_ma_7", "FLOAT"),
    SchemaField("price_ma_15", "FLOAT"),
    SchemaField("price_ma_30", "FLOAT"),
    SchemaField("original_price", "FLOAT"),
    SchemaField("discount_price", "FLOAT"),
]
