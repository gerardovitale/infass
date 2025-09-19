{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      'field': 'year_month_period',
      'data_type': 'date',
      'granularity': 'month'
    },
    cluster_by = ['category_key'],
    on_schema_change='sync_all_columns'
) }}

WITH base AS (SELECT year_month_period,
                     category_key,
                     price_variation_percent,
                     orig_price_variation_percent
              FROM {{ ref("monthly_products") }}
              {% if is_incremental() %}
              WHERE year_month_period >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY), MONTH)
              {% endif %}
              )

SELECT year_month_period,
       category_key,
       COUNT(*)                                                  AS product_count,

       -- Price variation stats
       AVG(price_variation_percent)                              AS avg_price_var_pct,
       STDDEV_SAMP(price_variation_percent)                      AS stdev_price_var_pct,
       AVG(GREATEST(LEAST(price_variation_percent, 1), -1))      AS avg_clipped_price_var_pct,

       -- Original price variation stats
       AVG(orig_price_variation_percent)                         AS avg_orig_price_var_pct,
       STDDEV_SAMP(orig_price_variation_percent)                 AS stdev_orig_price_var_pct,
       AVG(GREATEST(LEAST(orig_price_variation_percent, 1), -1)) AS avg_clipped_orig_price_var_pct
FROM base
GROUP BY 1, 2
