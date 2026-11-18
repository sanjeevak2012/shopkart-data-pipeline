
-- =====================================================
-- SHOPKART: BRONZE TO SILVER
-- Transformation: Payments
-- =====================================================

CREATE OR REPLACE TABLE
  `data-engineering-b55.shopkart_silver.payments`
AS

SELECT
    payment_id,
    order_id,
    attempt_number,
    payment_date,

    -- Standardize payment method
    NULLIF(UPPER(TRIM(payment_method)), '') AS payment_method,

    payment_amount,

    -- Standardize payment status
    NULLIF(UPPER(TRIM(payment_status)), '') AS payment_status,

    transaction_id,

    -- Standardize payment provider
    NULLIF(UPPER(TRIM(payment_provider)), '') AS payment_provider,

    created_at,
    updated_at,

    -- Track Silver transformation time
    CURRENT_TIMESTAMP() AS silver_loaded_at

FROM
  `data-engineering-b55.shopkart_bronze.payments`;
