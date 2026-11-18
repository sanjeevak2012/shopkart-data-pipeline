
-- =====================================================
-- SHOPKART: SILVER TO GOLD
-- Gold Table: Inventory Analytics
-- Business Rule: Calculate stock from signed movements
-- =====================================================

CREATE OR REPLACE TABLE
  `data-engineering-b55.shopkart_gold.inventory_analytics`
AS

WITH inventory_summary AS (

    SELECT

        product_id,

        SUM(
            IF(event_type = 'OPENING_BALANCE',
               quantity_change, 0)
        ) AS opening_stock,

        SUM(
            IF(event_type = 'SALE',
               ABS(quantity_change), 0)
        ) AS units_sold,

        SUM(
            IF(event_type = 'RETURN',
               quantity_change, 0)
        ) AS units_returned,

        SUM(
            IF(event_type = 'RESTOCK',
               quantity_change, 0)
        ) AS units_restocked,

        SUM(
            IF(event_type = 'ADJUSTMENT',
               quantity_change, 0)
        ) AS adjustment_net,

        COUNT(*) AS total_inventory_events,

        SUM(quantity_change) AS calculated_current_stock,

        MAX(event_timestamp) AS last_inventory_event

    FROM
        `data-engineering-b55.shopkart_silver.inventory_events`

    GROUP BY
        product_id
)

SELECT

    p.product_id,
    p.product_name,
    p.category,

    COALESCE(i.opening_stock, 0) AS opening_stock,

    COALESCE(i.units_sold, 0) AS units_sold,

    COALESCE(i.units_returned, 0) AS units_returned,

    COALESCE(i.units_restocked, 0) AS units_restocked,

    COALESCE(i.adjustment_net, 0) AS adjustment_net,

    COALESCE(i.total_inventory_events, 0)
        AS total_inventory_events,

    COALESCE(i.calculated_current_stock, 0)
        AS calculated_current_stock,

    i.last_inventory_event,

    CURRENT_TIMESTAMP() AS gold_loaded_at

FROM
    `data-engineering-b55.shopkart_silver.products` p

LEFT JOIN
    inventory_summary i
    ON p.product_id = i.product_id;
