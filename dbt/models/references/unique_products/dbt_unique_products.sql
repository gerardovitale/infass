{{ config(materialized='table') }}

SELECT
    GENERATE_UUID() AS product_id,
    name,
    size,
    STRING_AGG(category, " | ") AS categories,
    STRING_AGG(subcategory, " | ") AS subcategories,
    MAX_BY(price, date) AS price,
    NULL AS image_url
FROM {{ source('infass', 'merc') }}
WHERE date = {{ get_last_saturday_date() }}
    AND category IS NOT NULL
    AND subcategory IS NOT NULL
GROUP BY name, size
