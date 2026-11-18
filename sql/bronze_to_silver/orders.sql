
-- =====================================================
-- SHOPKART: BRONZE TO SILVER
-- Transformation: Orders
-- =====================================================

CREATE OR REPLACE TABLE
  `data-engineering-b55.shopkart_silver.orders`
AS

SELECT
    order_id,
    customer_id,
    order_date,

    -- Standardize order status
    NULLIF(UPPER(TRIM(order_status)), '') AS order_status,

    total_quantity,
    order_total,

    -- Track Silver transformation time
    CURRENT_TIMESTAMP() AS silver_loaded_at

FROM
  `data-engineering-b55.shopkart_bronze.orders`;
