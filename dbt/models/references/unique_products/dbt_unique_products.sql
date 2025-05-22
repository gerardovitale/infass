{{ config(materialized='view') }}

WITH distinct_prod_cat_comb AS (
    SELECT DISTINCT
        name,
        size,
        category,
        subcategory
    FROM {{ source('infass', 'merc') }}
    WHERE date = {{ get_last_saturday_date() }}
        AND category IS NOT NULL
        AND subcategory IS NOT NULL
)

SELECT
    GENERATE_UUID() AS product_id,
    name,
    size,
    STRING_AGG(category, " | ") AS categories,
    STRING_AGG(subcategory, " | ") AS subcategories,
    count(*) as category_count
FROM distinct_prod_cat_comb
GROUP BY name, size
