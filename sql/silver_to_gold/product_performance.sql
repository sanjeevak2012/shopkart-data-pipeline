
-- =====================================================
-- SHOPKART: SILVER TO GOLD
-- Gold Table: Product Performance
-- Business Rule: Include DELIVERED orders only
-- =====================================================

CREATE OR REPLACE TABLE
  `data-engineering-b55.shopkart_gold.product_performance`
AS

WITH product_sales AS (

    SELECT
        p.product_id,
        p.product_name,
        p.category,

        SUM(oi.quantity) AS total_units_sold,

        COUNT(DISTINCT o.order_id) AS total_orders,

        SUM(oi.line_total) AS total_revenue,

        SAFE_DIVIDE(
            SUM(oi.line_total),
            SUM(oi.quantity)
        ) AS average_selling_price

    FROM
        `data-engineering-b55.shopkart_silver.orders` o

    INNER JOIN
        `data-engineering-b55.shopkart_silver.order_items` oi
        ON o.order_id = oi.order_id

    INNER JOIN
        `data-engineering-b55.shopkart_silver.products` p
        ON oi.product_id = p.product_id

    WHERE
        o.order_status = 'DELIVERED'

    GROUP BY
        p.product_id,
        p.product_name,
        p.category
)

SELECT
    product_id,
    product_name,
    category,
    total_units_sold,
    total_orders,
    total_revenue,
    average_selling_price,

    DENSE_RANK() OVER (
        ORDER BY total_revenue DESC
    ) AS revenue_rank,

    CURRENT_TIMESTAMP() AS gold_loaded_at

FROM product_sales;
