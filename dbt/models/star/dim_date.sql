{{ config(materialized='table') }}

{% set start_date = var('date_spine_start', '2024-11-01') %}
{% set end_date   = var('date_spine_end', '2035-12-31') %}

with spine as (
  {{ dbt_utils.date_spine(
      datepart="day",
      start_date="cast('" ~ start_date ~ "' as datetime)",
      end_date="cast('" ~ end_date ~ "' as datetime)"
  ) }}
)

select cast(date_day as date)                                       as date,
       cast(format_date('%Y%m%d', cast(date_day as date)) as int64) as date_key,

       -- Year / quarter / month
       extract(year from date_day)                                  as year,
       extract(quarter from date_day)                               as quarter,
       extract(month from date_day)                                 as month,
       cast(format_date('%Y%m', cast(date_day as date)) as int64)   as month_key,
       format_date('%B', cast(date_day as date))                    as month_name,

       -- ISO week (Mon–Sun) and week boundaries
       extract(isoyear from date_day)                               as iso_year,
       extract(isoweek from date_day)                               as iso_week,
       date_trunc(cast(date_day as date), week(monday))             as week_start_date,
       date_add(date_trunc(cast(date_day as date), week(monday)), interval 6
                day)                                                as week_end_date,
       format_date('%G%V', cast(date_day as date))                  as iso_week_key,

       -- Day fields (1=Mon..7=Sun)
       mod(extract(dayofweek from date_day) + 5, 7) + 1             as day_of_week,
       extract(day from date_day)                                   as day_of_month,
       extract(dayofyear from date_day)                             as day_of_year,
       case
           when mod(extract(dayofweek from date_day) + 5, 7) + 1 in (6, 7) then true
           else false end                                           as is_weekend,

       -- Period starts/ends
       date_trunc(cast(date_day as date), month)                    as month_start_date,
       date_sub(date_add(date_trunc(cast(date_day as date), month), interval 1 month), interval 1
                day)                                                as month_end_date,
       date_trunc(cast(date_day as date), quarter)                  as quarter_start_date,
       date_sub(date_add(date_trunc(cast(date_day as date), quarter), interval 1 quarter), interval 1
                day)                                                as quarter_end_date,
       date_trunc(cast(date_day as date), year)                     as year_start_date,
       date_sub(date_add(date_trunc(cast(date_day as date), year), interval 1 year), interval 1
                day)                                                as year_end_date
from spine
order by date
