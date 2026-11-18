
-- =====================================================
-- SHOPKART: SILVER TO GOLD
-- Gold Table: Daily Sales Summary
-- Business Rule: Count DELIVERED orders as sales
-- =====================================================

CREATE OR REPLACE TABLE
  `data-engineering-b55.shopkart_gold.daily_sales_summary`
AS

SELECT
    DATE(order_date) AS sales_date,

    COUNT(DISTINCT order_id) AS total_orders,

    COUNT(DISTINCT customer_id) AS unique_customers,

    SUM(total_quantity) AS total_units_sold,

    SUM(order_total) AS total_revenue,

    AVG(order_total) AS average_order_value,

    CURRENT_TIMESTAMP() AS gold_loaded_at

FROM
  `data-engineering-b55.shopkart_silver.orders`

WHERE
    order_status = 'DELIVERED'

GROUP BY
    sales_date;
