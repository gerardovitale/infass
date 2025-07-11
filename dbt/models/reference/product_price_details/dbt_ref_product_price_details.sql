{{
  config(
    materialized = 'incremental',
    unique_key = ['id', 'date'],
    merge_update_columns = [
      'price',
      'sma7',
      'sma15',
      'sma30',
    ],
    partition_by = {
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["id"]
  )
}}
SELECT
    merc.date,
    products.id,
    merc.price,
    merc.price_ma_7 AS sma7,
    merc.price_ma_15 AS sma15,
    merc.price_ma_30 AS sma30,
    CAST({{ current_timestamp() }} AS DATE) AS created_at,
    CAST({{ current_timestamp() }} AS DATE) AS updated_at
FROM {{ source('infass', 'merc') }} AS merc
LEFT JOIN {{ ref('dbt_ref_products') }} AS products
  ON products.name = merc.name
  AND products.size = merc.size
{% if is_incremental() %}
WHERE merc.date >= (SELECT MAX(date) FROM {{ this }})
{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY merc.date, merc.name, merc.size ORDER BY merc.date DESC) = 1
