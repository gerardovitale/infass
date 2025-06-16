WITH null_counts AS (
    SELECT
        date,
        COUNT(*) AS total_rows,
        COUNT(DISTINCT category) AS category_count,
        COUNT(DISTINCT subcategory) AS subcategory_count,
        COUNT(DISTINCT CASE WHEN NOT discount_price IS NULL THEN name END) AS discount_product_count,
        COUNTIF(dedup_id IS NULL) AS null_dedup_id,
        COUNTIF(name IS NULL) AS null_name,
        COUNTIF(size IS NULL) AS null_size,
        COUNTIF(category IS NULL) AS null_category,
        COUNTIF(subcategory IS NULL) AS null_subcategory,
        COUNTIF(price IS NULL) AS null_price,
        COUNTIF(price_ma_7 IS NULL) AS null_price_ma_7,
        COUNTIF(price_ma_15 IS NULL) AS null_price_ma_15,
        COUNTIF(price_ma_30 IS NULL) AS null_price_ma_30,
        COUNTIF(original_price IS NULL) AS null_original_price,
        COUNTIF(discount_price IS NULL) AS null_discount_price
    FROM {{ source('infass', 'merc') }}
    GROUP BY
        1
),

duplicate_counts AS (
    SELECT
        date,
        SUM(duplicate_count - 1) AS duplicate_rows
    FROM (
        SELECT
            date,
            COUNT(*) AS duplicate_count
        FROM {{ source('infass', 'merc') }}
        GROUP BY
            date,
            name,
            size,
            category,
            subcategory,
            original_price,
            discount_price
    )
    WHERE
        duplicate_count > 1
    GROUP BY
        date
)

SELECT
    nc.date,
    nc.total_rows,
    COALESCE(dc.duplicate_rows, 0) AS duplicate_rows,
    SAFE_DIVIDE(COALESCE(dc.duplicate_rows, 0), nc.total_rows) AS percent_duplicate_rows,
    nc.category_count,
    nc.subcategory_count,
    nc.discount_product_count,
    (
        nc.total_rows / LAG(nc.total_rows) OVER (ORDER BY nc.date) - 1
    ) AS total_rows_change_percentage,
    (
        nc.discount_product_count / LAG(nc.discount_product_count) OVER (ORDER BY nc.date) - 1
    ) AS discount_product_count_change_percentage,
    SAFE_DIVIDE(nc.null_dedup_id, nc.total_rows) AS percent_null_dedup_id,
    SAFE_DIVIDE(nc.null_name, nc.total_rows) AS percent_null_name,
    SAFE_DIVIDE(nc.null_size, nc.total_rows) AS percent_null_size,
    SAFE_DIVIDE(nc.null_category, nc.total_rows) AS percent_null_category,
    SAFE_DIVIDE(nc.null_subcategory, nc.total_rows) AS percent_null_subcategory,
    SAFE_DIVIDE(nc.null_price, nc.total_rows) AS percent_null_price,
    SAFE_DIVIDE(nc.null_price_ma_7, nc.total_rows) AS percent_null_price_ma_7,
    SAFE_DIVIDE(nc.null_price_ma_15, nc.total_rows) AS percent_null_price_ma_15,
    SAFE_DIVIDE(nc.null_price_ma_30, nc.total_rows) AS percent_null_price_ma_30,
    SAFE_DIVIDE(nc.null_original_price, nc.total_rows) AS percent_null_original_price,
    SAFE_DIVIDE(nc.null_discount_price, nc.total_rows) AS percent_null_discount_price,
    nc.null_dedup_id,
    nc.null_name,
    nc.null_size,
    nc.null_category,
    nc.null_subcategory,
    nc.null_price,
    nc.null_price_ma_7,
    nc.null_price_ma_15,
    nc.null_price_ma_30,
    nc.null_original_price,
    nc.null_discount_price
FROM null_counts AS nc
LEFT JOIN duplicate_counts AS dc
    ON nc.date = dc.date
ORDER BY
    nc.date DESC
