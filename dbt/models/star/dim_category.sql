{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='category_nk',
    on_schema_change='sync'
) }}

with src as (
  select distinct
    {{ dbt_utils.generate_surrogate_key(["category","subcategory"]) }} as category_nk,
    category,
    subcategory
  from {{ source('infass', 'incremental_merc') }}
  where category is not null or subcategory is not null
)

select
  category_nk as category_key,
  category_nk,
  category,
  subcategory
from src
