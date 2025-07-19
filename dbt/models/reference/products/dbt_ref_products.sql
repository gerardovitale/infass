{{ config(materialized='incremental', unique_key='id') }}
SELECT
    TO_HEX(MD5(CONCAT(name, size))) AS id,
    name,
    size,
    STRING_AGG(DISTINCT category, ' | ') AS categories,
    STRING_AGG(DISTINCT subcategory, ' | ') AS subcategories,
    MAX_BY(price, date) AS price,
    MAX_BY(image_url, date) AS image_url
FROM {{ source('infass', 'merc') }}
WHERE
    category IS NOT NULL
    AND subcategory IS NOT NULL
    {% if is_incremental() %}
    AND TO_HEX(MD5(CONCAT(name, size))) NOT IN (SELECT id FROM {{ this }})
    {% endif %}
GROUP BY
    name,
    size
