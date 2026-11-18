
-- =====================================================
-- SHOPKART: SILVER TO GOLD
-- Gold Table: Customer Analytics
-- Business Rule: Include all customers
-- Revenue: DELIVERED orders only
-- =====================================================

CREATE OR REPLACE TABLE
  `data-engineering-b55.shopkart_gold.customer_analytics`
AS

WITH customer_sales AS (

    SELECT
        customer_id,

        COUNT(DISTINCT order_id) AS total_delivered_orders,

        SUM(order_total) AS lifetime_revenue,

        AVG(order_total) AS average_order_value,

        MAX(DATE(order_date)) AS last_delivered_order_date

    FROM
        `data-engineering-b55.shopkart_silver.orders`

    WHERE
        order_status = 'DELIVERED'

    GROUP BY
        customer_id
)

SELECT
    c.customer_id,
    c.customer_name,
    c.city,
    c.state,
    c.customer_segment,

    COALESCE(
        cs.total_delivered_orders, 0
    ) AS total_delivered_orders,

    COALESCE(
        cs.lifetime_revenue, 0
    ) AS lifetime_revenue,

    cs.average_order_value,

    cs.last_delivered_order_date,

    DENSE_RANK() OVER (
        ORDER BY COALESCE(cs.lifetime_revenue, 0) DESC
    ) AS revenue_rank,

    CURRENT_TIMESTAMP() AS gold_loaded_at

FROM
    `data-engineering-b55.shopkart_silver.customers` c

LEFT JOIN
    customer_sales cs
    ON c.customer_id = cs.customer_id;
