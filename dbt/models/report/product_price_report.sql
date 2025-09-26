{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'product_key',
    on_schema_change = 'sync',
    cluster_by = ['product_key']
) }}

{% set scan_window = var('product_price_report_scan_window_days', 366) %}

with base as (
  select
    fact_date,
    product_key,
    min(price) as price
  from {{ ref('fact_product_price_daily') }}
  where fact_date >= date_sub(current_date(), interval {{ scan_window }} day)
  group by 1, 2
),

src as (
  select
    base.product_key,
    p.name,
    p.size,
    fact_date                                                as latest_date,
    price                                                    as latest_price,
    date_trunc(fact_date, month)                             as mtd_start_date,
    date_trunc(date_sub(fact_date, interval 2 month), month) as mtd3_start_date,
    date_trunc(date_sub(fact_date, interval 5 month), month) as mtd6_start_date,
    date_trunc(fact_date, year)                              as ytd_start_date
  from base
  join {{ ref('dim_product') }} as p
    on base.product_key = p.product_key
  qualify row_number() over (partition by base.product_key order by fact_date desc) = 1
)

select
  src.product_key,
  src.name,
  src.size,

  src.latest_date,
  src.latest_price,

  src.mtd_start_date,
  mtd.price                                                as mtd_price,
  (src.latest_price - mtd.price)                           as mtd_price_var_abs,
  safe_divide((src.latest_price - mtd.price), mtd.price)   as mtd_price_var_pct,

  src.mtd3_start_date,
  mtd3.price                                               as mtd3_price,
  (src.latest_price - mtd3.price)                          as mtd3_price_var_abs,
  safe_divide((src.latest_price - mtd3.price), mtd3.price) as mtd3_price_var_pct,

  src.mtd6_start_date,
  mtd6.price                                               as mtd6_price,
  (src.latest_price - mtd6.price)                          as mtd6_price_var_abs,
  safe_divide((src.latest_price - mtd6.price), mtd6.price) as mtd6_price_var_pct,

  src.ytd_start_date,
  ytd.price                                                as ytd_price,
  (src.latest_price - ytd.price)                           as ytd_price_var_abs,
  safe_divide((src.latest_price - ytd.price), ytd.price)   as ytd_price_var_pct
from src
left join base as mtd
  on  src.product_key    = mtd.product_key
  and src.mtd_start_date = mtd.fact_date
left join base as mtd3
  on src.product_key      = mtd3.product_key
  and src.mtd3_start_date = mtd3.fact_date
left join base as mtd6
  on  src.product_key     = mtd6.product_key
  and src.mtd6_start_date = mtd6.fact_date
left join base as ytd
  on  src.product_key    = ytd.product_key
  and src.ytd_start_date = ytd.fact_date
