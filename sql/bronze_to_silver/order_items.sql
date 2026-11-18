
-- =====================================================
-- SHOPKART: BRONZE TO SILVER
-- Transformation: Order Items
-- =====================================================

CREATE OR REPLACE TABLE
  `data-engineering-b55.shopkart_silver.order_items`
AS

SELECT
    order_id,
    product_id,

    quantity,

    -- Preserve exact financial precision
    unit_price,
    line_total,

    -- Track Silver transformation time
    CURRENT_TIMESTAMP() AS silver_loaded_at

FROM
  `data-engineering-b55.shopkart_bronze.order_items`;
