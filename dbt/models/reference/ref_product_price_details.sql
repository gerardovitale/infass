{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    unique_key = ['id', 'date'],
    partition_by = {
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["id"]
  )
}}

{% set max_months = 6 %}
{% set backfill_months = var('sma_backfill_months', max_months) %}

WITH base AS (
    SELECT
        d.date,
        fact.product_key AS id,
        fact.price,
    FROM {{ ref('fact_product_price_daily') }} AS fact
    LEFT JOIN {{ ref('dim_date') }} AS d
        ON d.date_key = fact.date_key
    {% if is_incremental() %}
    WHERE d.date >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ backfill_months }} MONTH)
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY d.date, fact.product_key ORDER BY d.date DESC) = 1
),

sma AS (
    SELECT
        date,
        id,
        price,

        AVG(price) OVER (
            PARTITION BY id
            ORDER BY UNIX_DATE(date)
            RANGE BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS sma7,

        AVG(price) OVER (
            PARTITION BY id
            ORDER BY UNIX_DATE(date)
            RANGE BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS sma15,

        AVG(price) OVER (
            PARTITION BY id
            ORDER BY UNIX_DATE(date)
            RANGE BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS sma30
    FROM base
)

SELECT
    date,
    id,
    price,
    sma7,
    sma15,
    sma30
FROM sma
