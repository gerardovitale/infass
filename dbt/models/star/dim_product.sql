{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='product_nk',
    on_schema_change='sync'
) }}

{% set days_back = var('dim_product_days_back', 30) %}

with src as (
  select
    name,
    size,
    image_url,
    cast(date as date) as obs_date
  from {{ source('infass', 'stg_merc') }}
  {% if is_incremental() %}
    where date >= date_sub(
      (select coalesce(max(last_seen_date), date '1900-01-01') from {{ this }}),
      interval {{ days_back }} day
    )
  {% endif %}
),

agg_src as (
  select
    {{ dbt_utils.generate_surrogate_key(["name", "size"]) }} as product_nk,
    name,
    size,

    -- prefer the most recently observed image
    (array_agg(image_url ignore nulls order by obs_date desc limit 1))[safe_offset(0)] as new_image_url,

    min(obs_date) as min_obs_date,
    max(obs_date) as max_obs_date
  from src
  group by 1,2,3
),

-- Bring in existing rows to preserve first_seen and extend last_seen
hist as (
  {% if is_incremental() %}
  select
    product_nk,
    first_seen_date,
    last_seen_date,
    image_url as old_image_url
  from {{ this }}
  {% else %}
    -- empty, typed placeholder so downstream SELECTs compile
  select
    cast(null as string)  as product_nk,
    cast(null as date)    as first_seen_date,
    cast(null as date)    as last_seen_date,
    cast(null as string)  as old_image_url
  limit 0
  {% endif %}
),

upserts as (
  select
    incoming.product_nk,
    incoming.name,
    incoming.size,

    -- Type-1: overwrite with the newest non-null image if present, else keep old
    coalesce(incoming.new_image_url, hist.old_image_url) as image_url,

    -- Preserve earliest seen; extend latest seen
    least(coalesce(hist.first_seen_date, incoming.min_obs_date), incoming.min_obs_date) as first_seen_date,
    greatest(coalesce(hist.last_seen_date,  incoming.max_obs_date), incoming.max_obs_date) as last_seen_date
  from agg_src as incoming
  left join hist using (product_nk)
)

select
  {{ dbt_utils.generate_surrogate_key(['product_nk']) }} as product_key,
  product_nk,
  name,
  size,
  image_url,
  first_seen_date,
  last_seen_date
from upserts
