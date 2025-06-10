WITH merc_with_year_month_agg AS (
    SELECT
        TIMESTAMP_TRUNC(date, MONTH) AS year_month_period,
        name,
        size,
        category,
        subcategory,
        MIN_BY(date, date) AS earliest_date,
        MAX_BY(date, date) AS latest_date,
        MIN_BY(price, date) AS earliest_price,
        MAX_BY(price, date) AS latest_price,
        MIN_BY(original_price, date) AS earliest_orig_price,
        MAX_BY(original_price, date) AS latest_orig_price
    FROM {{ source('infass', 'merc') }}
    GROUP BY
        year_month_period,
        name,
        size,
        category,
        subcategory
)

SELECT
    year_month_period,
    name,
    size,
    category,
    subcategory,
    CAST({{ current_timestamp() }} AS DATE) AS processing_date,
    earliest_date,
    latest_date,
    earliest_price,
    latest_price,
    (
        latest_price - earliest_price
    ) AS price_variation_abs,
    {{ get_variation_percent('latest_price', 'earliest_price') }} AS price_variation_percent,
    earliest_orig_price,
    latest_orig_price,
    (
        latest_orig_price - earliest_orig_price
    ) AS orig_price_variation_abs,
    {{ get_variation_percent('latest_orig_price', 'earliest_orig_price') }} AS orig_price_variation_percent
FROM merc_with_year_month_agg
