SELECT
    STRFTIME(CAST(order_date AS DATE), '%Y') AS order_period,
    SUM(amount) AS total_amount
FROM {{ ref('combined_orders') }}
GROUP BY order_period
ORDER BY order_period

