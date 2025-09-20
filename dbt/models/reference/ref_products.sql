{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'id',
    on_schema_change = 'sync',
    cluster_by = ['id']
) }}

{% set max_days = 120 %}
{% set recent_days = var('ref_latest_price_days', max_days) %}

WITH src AS (
    SELECT
        product_key,
        STRING_AGG(DISTINCT category, ' | ') AS categories,
        STRING_AGG(DISTINCT subcategory, ' | ') AS subcategories,
        MAX_BY(price, date) AS price,
    FROM {{ ref('fact_product_price_daily') }}
    JOIN {{ ref('dim_date') }} USING (date_key)
    LEFT JOIN {{ ref ('dim_category') }} USING (category_key)
    WHERE
        category IS NOT NULL
        AND subcategory IS NOT NULL
        {% if is_incremental() %}
        AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ recent_days }} DAY)
        {% endif %}
    GROUP BY product_key
)

SELECT
    src.product_key AS id,
    p.name,
    p.size,
    src.categories,
    src.subcategories,
    src.price,
    p.image_url
FROM src
LEFT JOIN {{ ref('dim_product') }} AS p
    ON src.product_key = p.product_key
