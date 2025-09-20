{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      'field': 'year_month_period',
      'data_type': 'date',
      'granularity': 'month'
    },
    cluster_by = ['direction'],
    on_schema_change='sync_all_columns'
) }}

{% set TOP_N = var('top_n', 5) %}
{% set max_months = 3 %}
{% set backfill_months = var('monthly_product_top_movers_backfill_months', max_months) %}

WITH base AS (
  SELECT
    year_month_period,
    product_key,
    category_key,
    price_variation_percent
  FROM {{ ref("monthly_products") }}
  {% if is_incremental() %}
  WHERE year_month_period >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL {{ backfill_months }} MONTH), MONTH)
  {% endif %}
),

ranked_increases AS (
  SELECT
    *,
    DENSE_RANK() OVER (
      PARTITION BY year_month_period
      ORDER BY price_variation_percent DESC
    ) AS rank
  FROM base
),

ranked_decreases AS (
  SELECT
    *,
    DENSE_RANK() OVER (
      PARTITION BY year_month_period
      ORDER BY price_variation_percent ASC
    ) AS rank
  FROM base
),

top_increases AS (
  SELECT
    *,
    'increase' AS direction
  FROM ranked_increases
  WHERE rank <= {{ TOP_N }}
),

top_decreases AS (
  SELECT
    *,
    'decrease' AS direction
  FROM ranked_decreases
  WHERE rank <= {{ TOP_N }}
)

SELECT
  year_month_period,
  product_key,
  category_key,
  price_variation_percent,
  direction,
  rank,
  CURRENT_DATE() AS processing_date
FROM (
  SELECT * FROM top_increases
  UNION ALL
  SELECT * FROM top_decreases
)
