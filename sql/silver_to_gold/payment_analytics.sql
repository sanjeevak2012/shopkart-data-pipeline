
-- =====================================================
-- SHOPKART: SILVER TO GOLD
-- Gold Table: Payment Analytics
-- Grain: One row per payment method and provider
-- =====================================================

CREATE OR REPLACE TABLE
  `data-engineering-b55.shopkart_gold.payment_analytics`
AS

SELECT

    payment_method,
    payment_provider,

    COUNT(*) AS total_attempts,

    COUNT(DISTINCT order_id) AS unique_orders,

    COUNTIF(payment_status = 'SUCCESS')
        AS successful_attempts,

    COUNTIF(payment_status = 'FAILED')
        AS failed_attempts,

    COUNTIF(payment_status = 'PENDING')
        AS pending_attempts,

    COUNT(DISTINCT IF(
        payment_status = 'SUCCESS',
        order_id,
        NULL
    )) AS successful_orders,

    SUM(IF(
        payment_status = 'SUCCESS',
        payment_amount,
        0
    )) AS successful_payment_amount,

    AVG(IF(
        payment_status = 'SUCCESS',
        payment_amount,
        NULL
    )) AS average_successful_payment,

    ROUND(
        SAFE_DIVIDE(
            COUNTIF(payment_status = 'SUCCESS'),
            COUNT(*)
        ) * 100,
        2
    ) AS success_rate_pct,

    CURRENT_TIMESTAMP() AS gold_loaded_at

FROM
    `data-engineering-b55.shopkart_silver.payments`

GROUP BY
    payment_method,
    payment_provider;
