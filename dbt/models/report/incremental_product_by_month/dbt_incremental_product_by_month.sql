{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      'field': 'year_month_period',
      'data_type': 'date',
      'granularity': 'month'
    },
    cluster_by = ['name','size'],
    on_schema_change = 'sync_all_columns'
  )
}}

{% set month_start %} DATE_TRUNC(CURRENT_DATE(), MONTH) {% endset %}
{% set month_end %} TIMESTAMP_ADD({{ month_start }}, INTERVAL 1 MONTH) {% endset %}

WITH year_month_agg_merc AS (
    SELECT
        DATE_TRUNC(date, MONTH) AS year_month_period,
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
    FROM {{ source('infass', 'incremental_merc') }}

    {% if is_incremental() %}
    WHERE date >= {{ month_start }} AND date < {{ month_end }}
    {% endif %}

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
    latest_price - earliest_price AS price_variation_abs,
    {{ get_variation_percent('latest_price', 'earliest_price') }} AS price_variation_percent,

    earliest_orig_price,
    latest_orig_price,
    latest_orig_price - earliest_orig_price AS orig_price_variation_abs,
    {{ get_variation_percent('latest_orig_price', 'earliest_orig_price') }} AS orig_price_variation_percent
FROM year_month_agg_merc
