{{ config(
  materialized='incremental',
  unique_key=['date_key','product_key','category_key','dedup_id'],
  incremental_strategy = 'insert_overwrite',
    partition_by = {
      'field': 'fact_date',
      'data_type': 'date',
      'granularity': 'day'
    },
    cluster_by = ['product_key','category_key'],
  on_schema_change='sync'
) }}

{% set days_back = var('fact_days_back', 30) %}

-- Source rows (optionally windowed on incremental runs)
with src as (
  select
    {{ dbt_utils.generate_surrogate_key(["name", "size"]) }} as product_nk,
    {{ dbt_utils.generate_surrogate_key(["category","subcategory"]) }} as category_nk,
    cast(date as date) as date,
    dedup_id,
    name,
    size,
    category,
    subcategory,
    price,
    original_price,
    discount_price
  from {{ source('infass', 'incremental_merc') }}
  {% if is_incremental() %}
    where date >= date_sub(
      (select coalesce(max_date, date '1900-01-01')
       from (select MAX(date) as max_date from {{ this }} as t
       join {{ ref('dbt_dim_date') }} d on d.date_key = t.date_key)),
      interval {{ days_back }} day
    )
  {% endif %}
)

select
  d.date_key,
  p.product_key,
  c.category_key,
  src.dedup_id,

  -- Measures
  src.price,
  src.original_price,
  src.discount_price,
  (src.discount_price is not null and src.discount_price < src.original_price) as is_discounted,
  (src.original_price - src.price) as discount_abs,
  safe_divide(src.original_price - src.price, src.original_price) as discount_pct,

  -- Audit
  src.date as fact_date,
  current_timestamp() as load_ts
from src
join {{ ref('dbt_dim_date') }}     d on d.date = src.date
join {{ ref('dbt_dim_product') }}  p on p.product_nk  = src.product_nk
join {{ ref('dbt_dim_category') }} c on c.category_nk = src.category_nk
