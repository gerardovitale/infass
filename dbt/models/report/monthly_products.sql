{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      'field': 'year_month_period',
      'data_type': 'date',
      'granularity': 'month'
    },
    cluster_by = ['product_key'],
    on_schema_change = 'sync_all_columns'
  )
}}

{% set month_start %} DATE_TRUNC(CURRENT_DATE(), MONTH) {% endset %}
{% set month_end %} TIMESTAMP_ADD({{ month_start }}, INTERVAL 1 MONTH) {% endset %}

WITH year_month_agg_merc AS (
    SELECT
        d.month_start_date             AS year_month_period,
        src.product_key,
        src.category_key,
        MIN_BY(d.date, d.date)         AS earliest_date,
        MAX_BY(d.date, d.date)         AS latest_date,
        MIN_BY(price, d.date)          AS earliest_price,
        MAX_BY(price, d.date)          AS latest_price,
        MIN_BY(original_price, d.date) AS earliest_orig_price,
        MAX_BY(original_price, d.date) AS latest_orig_price
    FROM {{ ref("fact_product_price_daily") }} AS src
    LEFT JOIN {{ ref("dim_date") }} d ON d.date_key = src.date_key
    {% if is_incremental() %}
    WHERE d.date >= {{ month_start }} AND d.date < {{ month_end }}
    {% endif %}
    GROUP BY 1,2,3
)

SELECT
    year_month_period,
    product_key,
    category_key,
    CAST({{ current_timestamp() }} AS DATE) AS processing_date,
    earliest_date,
    latest_date,

    -- Price variations
    earliest_price,
    latest_price,
    latest_price - earliest_price AS price_variation_abs,
    {{ get_variation_percent('latest_price', 'earliest_price') }} AS price_variation_percent,

    -- Original Price variations
    earliest_orig_price,
    latest_orig_price,
    latest_orig_price - earliest_orig_price AS orig_price_variation_abs,
    {{ get_variation_percent('latest_orig_price', 'earliest_orig_price') }} AS orig_price_variation_percent
FROM year_month_agg_merc
