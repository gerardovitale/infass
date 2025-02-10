CREATE
OR REPLACE TABLE `inflation-assistant.infass.product_inflation_report` AS
WITH
  pre_calculation AS (
    SELECT
      name,
      size,
      MAX(category) AS category,
      CURRENT_DATE() AS processing_date,
      MIN_BY (date, date) AS earliest_date,
      MAX_BY (date, date) AS latest_date,
      MIN_BY (original_price, date) AS earliest_price,
      MAX_BY (original_price, date) AS latest_price,
      MIN_BY (date, original_price) AS min_original_price_date,
      MAX_BY (date, original_price) AS max_original_price_date,
      MIN(original_price) AS min_original_price,
      MAX(original_price) AS max_original_price,
    FROM
      `inflation-assistant.infass.merc`
    GROUP BY
      name,
      size
  )
SELECT
  *,
  (latest_price - earliest_price) AS inflation_abs,
  (latest_price - earliest_price) / earliest_price * 100 AS inflation_percent,
  (max_original_price - min_original_price) AS max_price_variation_abs,
  (max_original_price - min_original_price) / min_original_price * 100 AS max_price_variation_percent
FROM
  pre_calculation
ORDER BY
  inflation_percent DESC;