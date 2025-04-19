WITH
  pre_calculation AS (
    SELECT
      name,
      size,
      MAX(category) AS category,
      {{ date_trunc("day", current_timestamp()) }} AS processing_date,
      MIN_BY (date, date) AS earliest_date,
      MAX_BY (date, date) AS latest_date,
      MIN_BY (original_price, date) AS earliest_price,
      MAX_BY (original_price, date) AS latest_price,
      MIN(original_price) AS min_price,
      MAX(original_price) AS max_price,
    FROM
      {{ source('infass', 'merc') }}
    GROUP BY
      name,
      size
  )
  
SELECT
  *,
  round(latest_price - earliest_price, {{ var('default_round_precision') }}) AS inflation_abs,
  round((latest_price - earliest_price) / earliest_price, {{ var('default_round_precision') }}) AS inflation_percent,
  round(max_price - min_price, {{ var('default_round_precision') }}) AS max_price_variation_abs,
  round((max_price - min_price) / min_price, {{ var('default_round_precision') }}) AS max_price_variation_percent
FROM
  pre_calculation
ORDER BY
  inflation_percent DESC
