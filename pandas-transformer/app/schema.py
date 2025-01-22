INGESTION_SCHEMA = [
    "name",
    "original_price",
    "discount_price",
    "size",
    "category",
    "date",
]

DELTA_SCHEMA = [
    "date",
    "dedup_id",
    "name",
    "size",
    "category",
    "subcategory",
    "original_price",
    "prev_original_price",
    "discount_price",
    "is_fake_discount",
    "inflation_percent",
    "inflation_abs",
]
