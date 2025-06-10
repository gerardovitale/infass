{{
  config(
    materialized = "table",
    partition_by = {
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["product_id"]
  )
}}
SELECT
    merc.date,
    products.product_id,
    merc.price,
    merc.price_ma_7 AS sma7,
    merc.price_ma_15 AS sma15,
    merc.price_ma_30 AS sma30
FROM {{ source('infass', 'merc') }} AS merc
LEFT JOIN {{ ref('dbt_ref_products') }} AS products
    ON products.name = merc.name AND products.size = merc.size
WHERE
    date BETWEEN DATE_SUB({{ get_last_saturday_date() }}, INTERVAL '6' MONTH) AND {{ get_last_saturday_date() }}
