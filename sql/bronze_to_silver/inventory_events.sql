
-- =====================================================
-- SHOPKART: BRONZE TO SILVER
-- Transformation: Inventory Events
-- =====================================================

CREATE OR REPLACE TABLE
  `data-engineering-b55.shopkart_silver.inventory_events`
AS

SELECT
    event_id,
    product_id,

    -- Standardize inventory event type
    NULLIF(UPPER(TRIM(event_type)), '') AS event_type,

    -- Preserve positive and negative stock movements
    quantity_change,

    event_timestamp,

    -- Standardize reference ID
    NULLIF(TRIM(reference_id), '') AS reference_id,

    -- Standardize reason
    NULLIF(INITCAP(TRIM(reason)), '') AS reason,

    created_at,

    -- Track Silver transformation time
    CURRENT_TIMESTAMP() AS silver_loaded_at

FROM
  `data-engineering-b55.shopkart_bronze.inventory_events`;
