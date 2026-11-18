
-- =====================================================
-- SHOPKART: BRONZE TO SILVER
-- Transformation: Products
-- =====================================================

CREATE OR REPLACE TABLE
  `data-engineering-b55.shopkart_silver.products`
AS

SELECT
    product_id,

    -- Standardize product name
    NULLIF(INITCAP(TRIM(product_name)), '') AS product_name,

    -- Standardize category and subcategory
    NULLIF(INITCAP(TRIM(category)), '') AS category,
    NULLIF(INITCAP(TRIM(subcategory)), '') AS subcategory,

    -- Preserve exact numeric price
    price,

    -- Preserve stock quantity
    stock_quantity,

    -- Standardize product status
    NULLIF(UPPER(TRIM(product_status)), '') AS product_status,

    created_at,
    updated_at,

    -- Track Silver transformation time
    CURRENT_TIMESTAMP() AS silver_loaded_at

FROM
  `data-engineering-b55.shopkart_bronze.products`;
