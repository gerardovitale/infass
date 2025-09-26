{{ config(materialized = 'table') }}

{% set top_k = var('report_top_k', 10) %}

WITH src AS (
    SELECT
        ppr.product_key,
        ppr.name,
        ppr.size,
        p.categories,
        p.subcategories,
        ppr.latest_date,
        ppr.latest_price,
        ppr.ytd_start_date,
        ppr.ytd_price,
        ppr.ytd_price_var_abs,
        ppr.ytd_price_var_pct
    FROM {{ ref('product_price_report') }} ppr
    JOIN {{ ref('ref_products') }} p
        on ppr.product_key = p.id
    WHERE ppr.ytd_price_var_pct IS NOT NULL
        AND ppr.latest_date = (
            select MAX(latest_date) from {{ ref('product_price_report') }}
        )
),

top_movers AS (
    SELECT
        *,
        CASE
            WHEN ytd_price_var_pct > 0 THEN 'increase'
            WHEN ytd_price_var_pct < 0 THEN 'decrease'
            ELSE 'flat'
        END AS direction,
    FROM src
    QUALIFY ROW_NUMBER() OVER (
        ORDER BY
            ABS(ytd_price_var_pct) DESC,
            ABS(ytd_price_var_abs) DESC,
            latest_date DESC, product_key
    ) <= {{ top_k }}
)

SELECT
    product_key,
    name,
    size,
    categories,
    subcategories,
    latest_date,
    latest_price,
    ytd_start_date,
    ytd_price,
    ytd_price_var_abs,
    ytd_price_var_pct,
    direction
FROM top_movers
