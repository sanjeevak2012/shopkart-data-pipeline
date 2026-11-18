
-- =====================================================
-- SHOPKART: BRONZE TO SILVER
-- Transformation: Customers
-- =====================================================

CREATE OR REPLACE TABLE
  `data-engineering-b55.shopkart_silver.customers`
AS

SELECT
    customer_id,

    -- Standardize customer name
    NULLIF(INITCAP(TRIM(customer_name)), '') AS customer_name,

    -- Standardize email
    NULLIF(LOWER(TRIM(email)), '') AS email,

    -- Preserve phone as STRING
    NULLIF(TRIM(phone), '') AS phone,

    -- Standardize location
    NULLIF(INITCAP(TRIM(city)), '') AS city,
    NULLIF(INITCAP(TRIM(state)), '') AS state,

    -- Preserve pincode as STRING
    NULLIF(TRIM(pincode), '') AS pincode,

    registration_date,

    -- Standardize customer segment
    NULLIF(UPPER(TRIM(customer_segment)), '') AS customer_segment,

    is_active,

    created_at,
    updated_at,

    -- Track when Silver table was generated
    CURRENT_TIMESTAMP() AS silver_loaded_at

FROM
  `data-engineering-b55.shopkart_bronze.customers`;
