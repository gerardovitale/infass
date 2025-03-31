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
DELTA_SCHEMA_TYPES = {
    "date":                "datetime64[ns]",
    "dedup_id":            "int8",
    "original_price":      "float32",
    "prev_original_price": "float32",
    "discount_price":      "float32",
    "is_fake_discount":    "boolean",
    "inflation_percent":   "float32",
    "inflation_abs":       "float32",
}
