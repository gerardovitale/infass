CREATE
OR REPLACE TABLE `inflation-assistant.infass.quality_report` AS
WITH
  null_counts AS (
    SELECT
      date,
      COUNT(*) AS total_rows,
      COUNT(DISTINCT category) AS category_count,
      COUNT(DISTINCT subcategory) AS subcategory_count,
      COUNT(
        DISTINCT CASE
          WHEN discount_price IS NOT NULL THEN name
        END
      ) AS discount_product_count,
      COUNT(
        DISTINCT CASE
          WHEN is_fake_discount IS TRUE THEN name
        END
      ) AS fake_discount_product_count,
      -- ABSOLUTE NULL VALUES
      COUNTIF (dedup_id IS NULL) AS null_dedup_id,
      COUNTIF (name IS NULL) AS null_name,
      COUNTIF (size IS NULL) AS null_size,
      COUNTIF (category IS NULL) AS null_category,
      COUNTIF (subcategory IS NULL) AS null_subcategory,
      COUNTIF (price IS NULL) AS null_price,
      COUNTIF (prev_price IS NULL) AS null_prev_price,
      COUNTIF (price_ma_7 IS NULL) AS null_price_ma_7,
      COUNTIF (price_ma_15 IS NULL) AS null_price_ma_15,
      COUNTIF (price_ma_30 IS NULL) AS null_price_ma_30,
      COUNTIF (original_price IS NULL) AS null_original_price,
      COUNTIF (prev_original_price IS NULL) AS null_prev_original_price,
      COUNTIF (discount_price IS NULL) AS null_discount_price,
      COUNTIF (price_var_abs IS NULL) AS null_price_var_abs,
      COUNTIF (`price_var_%` IS NULL) AS null_price_var_percent,
      COUNTIF (original_price_var_abs IS NULL) AS null_original_price_var_abs,
      COUNTIF (`original_price_var_%` IS NULL) AS null_original_price_var_percent,
      COUNTIF (is_fake_discount IS NULL) AS null_is_fake_discount,
    FROM
      `inflation-assistant.infass.merc`
    GROUP BY
      1
  ),
  duplicate_counts AS (
    SELECT
      date,
      SUM(duplicate_count - 1) AS duplicate_rows
    FROM
      (
        SELECT
          date,
          COUNT(*) AS duplicate_count
        FROM
          `inflation-assistant.infass.merc`
        GROUP BY
          date,
          name,
          size,
          category,
          subcategory,
          original_price,
          discount_price
      )
    WHERE
      duplicate_count > 1
    GROUP BY
      date
  )
SELECT
  nc.date,
  nc.total_rows,
  COALESCE(dc.duplicate_rows, 0) AS duplicate_rows,
  COALESCE(dc.duplicate_rows, 0) / nc.total_rows AS percent_duplicate_rows,
  -- GENERAL COUNTS
  nc.category_count,
  nc.subcategory_count,
  nc.discount_product_count,
  nc.fake_discount_product_count,
  -- CHANGE RATIO
  (
    nc.total_rows / LAG (nc.total_rows) OVER (
      ORDER BY
        nc.date
    ) - 1
  ) AS total_rows_change_percentage,
  (
    nc.discount_product_count / LAG (nc.discount_product_count) OVER (
      ORDER BY
        nc.date
    ) - 1
  ) AS discount_product_count_change_percentage,
  -- PERCENTAGES NULL VALUES
  nc.null_dedup_id / nc.total_rows AS percent_null_dedup_id,
  nc.null_name / nc.total_rows AS percent_null_name,
  nc.null_size / nc.total_rows AS percent_null_size,
  nc.null_category / nc.total_rows AS percent_null_category,
  nc.null_subcategory / nc.total_rows AS percent_null_subcategory,
  nc.null_price / nc.total_rows AS percent_null_price,
  nc.null_prev_price / nc.total_rows AS percent_null_prev_price,
  nc.null_price_ma_7 / nc.total_rows AS percent_null_price_ma_7,
  nc.null_price_ma_15 / nc.total_rows AS percent_null_price_ma_15,
  nc.null_price_ma_30 / nc.total_rows AS percent_null_price_ma_30,
  nc.null_original_price / nc.total_rows AS percent_null_original_price,
  nc.null_prev_original_price / nc.total_rows AS percent_null_prev_original_price,
  nc.null_discount_price / nc.total_rows AS percent_null_discount_price,
  nc.null_price_var_abs / nc.total_rows AS percent_null_price_var_abs,
  nc.null_price_var_percent / nc.total_rows AS percent_null_price_var_percent,
  nc.null_original_price_var_abs / nc.total_rows AS percent_null_original_price_var_abs,
  nc.null_original_price_var_percent / nc.total_rows AS percent_null_original_price_var_percent,
  nc.null_is_fake_discount / nc.total_rows AS percent_null_is_fake_discount,

  -- ABSOLUTE NULL VALUES
  nc.null_dedup_id,
  nc.null_name,
  nc.null_size,
  nc.null_category,
  nc.null_subcategory,
  nc.null_price,
  nc.null_prev_price,
  nc.null_price_ma_7,
  nc.null_price_ma_15,
  nc.null_price_ma_30,
  nc.null_original_price,
  nc.null_prev_original_price,
  nc.null_discount_price,
  nc.null_price_var_abs,
  nc.null_price_var_percent,
  nc.null_original_price_var_abs,
  nc.null_original_price_var_percent,
  nc.null_is_fake_discount
FROM
  null_counts nc
  LEFT JOIN duplicate_counts dc ON nc.date = dc.date
ORDER BY
  nc.date DESC;
