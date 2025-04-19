CREATE
OR REPLACE TABLE `inflation-assistant.infass.product_inflation_per_month_report` AS
WITH
    monthly_prices AS (
        -- Get first and last price per product per month
        SELECT
            DATE_TRUNC (date, MONTH) AS month,
            name,
            size,
            category,
            subcategory,
            FIRST_VALUE (original_price) OVER (
                PARTITION BY
                    DATE_TRUNC (date, MONTH),
                    name,
                    size,
                    category,
                    subcategory
                ORDER BY
                    date ASC
            ) AS earliest_price,
            LAST_VALUE (original_price) OVER (
                PARTITION BY
                    DATE_TRUNC (date, MONTH),
                    name,
                    size,
                    category,
                    subcategory
                ORDER BY
                    date ASC ROWS BETWEEN UNBOUNDED PRECEDING
                    AND UNBOUNDED FOLLOWING
            ) AS latest_price
        FROM
            `inflation-assistant.infass.merc`
    )
SELECT
    month,
    name,
    size,
    category,
    subcategory,
    AVG(earliest_price) AS avg_earliest_price, -- Average first price of products in the month
    AVG(latest_price) AS avg_latest_price, -- Average last price of products in the month
    (AVG(latest_price) - AVG(earliest_price)) AS inflation_abs,
    SAFE_DIVIDE (
        AVG(latest_price) - AVG(earliest_price),
        AVG(earliest_price)
    ) AS inflation_rate
FROM
    monthly_prices
GROUP BY
    month,
    name,
    size,
    category,
    subcategory
ORDER BY
    month DESC;