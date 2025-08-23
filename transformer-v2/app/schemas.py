from google.cloud.bigquery import SchemaField

MERC_SCHEMA = [
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
