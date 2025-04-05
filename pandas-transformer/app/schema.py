INGESTION_SCHEMA = [
    "name",
    "original_price",
    "discount_price",
    "size",
    "category",
    "date",
]

DELTA_SCHEMA = {
    "date":                "datetime64[ns]",
    "dedup_id":            "int8",
    "name":                "string",
    "size":                "string",
    "category":            "category",
    "subcategory":         "category",
    "original_price":      "float32",
    "prev_original_price": "float32",
    "discount_price":      "float32",
    "is_fake_discount":    "boolean",
    "inflation_percent":   "float32",
    "inflation_abs":       "float32",
}
