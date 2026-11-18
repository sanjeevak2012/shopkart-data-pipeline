
from google.cloud import bigquery
import google.auth


PROJECT_ID = "data-engineering-b55"

SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]

credentials, _ = google.auth.default(scopes=SCOPES)

client = bigquery.Client(
    project=PROJECT_ID,
    credentials=credentials
)


GOLD_CHECKS = {

    # =====================================================
    # DAILY SALES SUMMARY
    # =====================================================

    "Daily sales reconciliation": """
        WITH expected AS (
            SELECT
                DATE(order_date) AS sales_date,
                COUNT(DISTINCT order_id) AS total_orders,
                COUNT(DISTINCT customer_id) AS unique_customers,
                SUM(total_quantity) AS total_units_sold,
                SUM(order_total) AS total_revenue,
                AVG(order_total) AS average_order_value
            FROM `data-engineering-b55.shopkart_silver.orders`
            WHERE order_status = 'DELIVERED'
            GROUP BY sales_date
        )

        SELECT COUNT(*) AS invalid_count
        FROM expected e
        FULL OUTER JOIN
            `data-engineering-b55.shopkart_gold.daily_sales_summary` g
        ON e.sales_date = g.sales_date
        WHERE
            e.sales_date IS NULL
            OR g.sales_date IS NULL
            OR e.total_orders IS DISTINCT FROM g.total_orders
            OR e.unique_customers IS DISTINCT FROM g.unique_customers
            OR e.total_units_sold IS DISTINCT FROM g.total_units_sold
            OR e.total_revenue IS DISTINCT FROM g.total_revenue
            OR e.average_order_value IS DISTINCT FROM g.average_order_value
    """,

    "Duplicate daily sales dates": """
        SELECT COUNT(*) AS invalid_count
        FROM (
            SELECT sales_date
            FROM `data-engineering-b55.shopkart_gold.daily_sales_summary`
            GROUP BY sales_date
            HAVING COUNT(*) > 1
        )
    """,

    "Daily sales NULL values": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_gold.daily_sales_summary`
        WHERE sales_date IS NULL
           OR total_orders IS NULL
           OR total_revenue IS NULL
           OR gold_loaded_at IS NULL
    """,

    # =====================================================
    # PRODUCT PERFORMANCE
    # =====================================================

    "Product performance reconciliation": """
        WITH expected AS (
            SELECT
                p.product_id,
                SUM(oi.quantity) AS total_units_sold,
                COUNT(DISTINCT o.order_id) AS total_orders,
                SUM(oi.line_total) AS total_revenue,
                SAFE_DIVIDE(
                    SUM(oi.line_total),
                    SUM(oi.quantity)
                ) AS average_selling_price
            FROM `data-engineering-b55.shopkart_silver.orders` o
            JOIN `data-engineering-b55.shopkart_silver.order_items` oi
                ON o.order_id = oi.order_id
            JOIN `data-engineering-b55.shopkart_silver.products` p
                ON oi.product_id = p.product_id
            WHERE o.order_status = 'DELIVERED'
            GROUP BY p.product_id
        )

        SELECT COUNT(*) AS invalid_count
        FROM expected e
        FULL OUTER JOIN
            `data-engineering-b55.shopkart_gold.product_performance` g
        ON e.product_id = g.product_id
        WHERE
            e.product_id IS NULL
            OR g.product_id IS NULL
            OR e.total_units_sold IS DISTINCT FROM g.total_units_sold
            OR e.total_orders IS DISTINCT FROM g.total_orders
            OR e.total_revenue IS DISTINCT FROM g.total_revenue
            OR e.average_selling_price IS DISTINCT FROM g.average_selling_price
    """,

    "Duplicate product IDs": """
        SELECT COUNT(*) AS invalid_count
        FROM (
            SELECT product_id
            FROM `data-engineering-b55.shopkart_gold.product_performance`
            GROUP BY product_id
            HAVING COUNT(*) > 1
        )
    """,

    "Invalid product revenue ranks": """
        SELECT COUNT(*) AS invalid_count
        FROM (
            SELECT
                product_id,
                revenue_rank,
                DENSE_RANK() OVER (
                    ORDER BY total_revenue DESC
                ) AS expected_rank
            FROM `data-engineering-b55.shopkart_gold.product_performance`
        )
        WHERE revenue_rank IS DISTINCT FROM expected_rank
    """,

    # =====================================================
    # CUSTOMER ANALYTICS
    # =====================================================

    "Customer coverage": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_silver.customers` c
        FULL OUTER JOIN
            `data-engineering-b55.shopkart_gold.customer_analytics` g
        ON c.customer_id = g.customer_id
        WHERE c.customer_id IS NULL
           OR g.customer_id IS NULL
    """,

    "Duplicate customer IDs in Gold": """
        SELECT COUNT(*) AS invalid_count
        FROM (
            SELECT customer_id
            FROM `data-engineering-b55.shopkart_gold.customer_analytics`
            GROUP BY customer_id
            HAVING COUNT(*) > 1
        )
    """,

    "Customer revenue reconciliation": """
        WITH expected AS (
            SELECT
                customer_id,
                COUNT(DISTINCT order_id) AS total_delivered_orders,
                SUM(order_total) AS lifetime_revenue,
                AVG(order_total) AS average_order_value,
                MAX(DATE(order_date)) AS last_delivered_order_date
            FROM `data-engineering-b55.shopkart_silver.orders`
            WHERE order_status = 'DELIVERED'
            GROUP BY customer_id
        )

        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_silver.customers` c
        JOIN `data-engineering-b55.shopkart_gold.customer_analytics` g
            ON c.customer_id = g.customer_id
        LEFT JOIN expected e
            ON c.customer_id = e.customer_id
        WHERE
            g.total_delivered_orders IS DISTINCT FROM
                COALESCE(e.total_delivered_orders, 0)
            OR g.lifetime_revenue IS DISTINCT FROM
                COALESCE(e.lifetime_revenue, 0)
            OR g.average_order_value IS DISTINCT FROM
                e.average_order_value
            OR g.last_delivered_order_date IS DISTINCT FROM
                e.last_delivered_order_date
    """,

    "Invalid customer revenue ranks": """
        SELECT COUNT(*) AS invalid_count
        FROM (
            SELECT
                revenue_rank,
                DENSE_RANK() OVER (
                    ORDER BY lifetime_revenue DESC
                ) AS expected_rank
            FROM `data-engineering-b55.shopkart_gold.customer_analytics`
        )
        WHERE revenue_rank IS DISTINCT FROM expected_rank
    """,

    # =====================================================
    # PAYMENT ANALYTICS
    # =====================================================

    "Payment analytics reconciliation": """
        WITH expected AS (
            SELECT
                payment_method,
                payment_provider,
                COUNT(*) AS total_attempts,
                COUNT(DISTINCT order_id) AS unique_orders,
                COUNTIF(payment_status = 'SUCCESS') AS successful_attempts,
                COUNTIF(payment_status = 'FAILED') AS failed_attempts,
                COUNTIF(payment_status = 'PENDING') AS pending_attempts,
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
                ) AS success_rate_pct
            FROM `data-engineering-b55.shopkart_silver.payments`
            GROUP BY payment_method, payment_provider
        )

        SELECT COUNT(*) AS invalid_count
        FROM expected e
        FULL OUTER JOIN
            `data-engineering-b55.shopkart_gold.payment_analytics` g
        ON e.payment_method = g.payment_method
        AND e.payment_provider = g.payment_provider
        WHERE
            e.total_attempts IS NULL
            OR g.total_attempts IS NULL
            OR e.total_attempts IS DISTINCT FROM g.total_attempts
            OR e.unique_orders IS DISTINCT FROM g.unique_orders
            OR e.successful_attempts IS DISTINCT FROM g.successful_attempts
            OR e.failed_attempts IS DISTINCT FROM g.failed_attempts
            OR e.pending_attempts IS DISTINCT FROM g.pending_attempts
            OR e.successful_orders IS DISTINCT FROM g.successful_orders
            OR e.successful_payment_amount IS DISTINCT FROM
                g.successful_payment_amount
            OR e.average_successful_payment IS DISTINCT FROM
                g.average_successful_payment
            OR e.success_rate_pct IS DISTINCT FROM g.success_rate_pct
    """,

    "Payment attempts reconciliation": """
        SELECT
            IF(
                (SELECT COUNT(*)
                 FROM `data-engineering-b55.shopkart_silver.payments`)
                =
                (SELECT SUM(total_attempts)
                 FROM `data-engineering-b55.shopkart_gold.payment_analytics`),
                0,
                1
            ) AS invalid_count
    """,

    "Payment status reconciliation": """
        SELECT
            IF(
                (SELECT COUNTIF(payment_status = 'SUCCESS')
                 FROM `data-engineering-b55.shopkart_silver.payments`)
                =
                (SELECT SUM(successful_attempts)
                 FROM `data-engineering-b55.shopkart_gold.payment_analytics`)
                AND
                (SELECT COUNTIF(payment_status = 'FAILED')
                 FROM `data-engineering-b55.shopkart_silver.payments`)
                =
                (SELECT SUM(failed_attempts)
                 FROM `data-engineering-b55.shopkart_gold.payment_analytics`)
                AND
                (SELECT COUNTIF(payment_status = 'PENDING')
                 FROM `data-engineering-b55.shopkart_silver.payments`)
                =
                (SELECT SUM(pending_attempts)
                 FROM `data-engineering-b55.shopkart_gold.payment_analytics`),
                0,
                1
            ) AS invalid_count
    """,

    # =====================================================
    # INVENTORY ANALYTICS
    # =====================================================

    "Inventory product coverage": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_silver.products` p
        FULL OUTER JOIN
            `data-engineering-b55.shopkart_gold.inventory_analytics` g
        ON p.product_id = g.product_id
        WHERE p.product_id IS NULL
           OR g.product_id IS NULL
    """,

    "Duplicate inventory product IDs": """
        SELECT COUNT(*) AS invalid_count
        FROM (
            SELECT product_id
            FROM `data-engineering-b55.shopkart_gold.inventory_analytics`
            GROUP BY product_id
            HAVING COUNT(*) > 1
        )
    """,

    "Inventory movement reconciliation": """
        WITH expected AS (
            SELECT
                product_id,
                SUM(IF(event_type = 'OPENING_BALANCE',
                    quantity_change, 0)) AS opening_stock,
                SUM(IF(event_type = 'SALE',
                    ABS(quantity_change), 0)) AS units_sold,
                SUM(IF(event_type = 'RETURN',
                    quantity_change, 0)) AS units_returned,
                SUM(IF(event_type = 'RESTOCK',
                    quantity_change, 0)) AS units_restocked,
                SUM(IF(event_type = 'ADJUSTMENT',
                    quantity_change, 0)) AS adjustment_net,
                COUNT(*) AS total_inventory_events,
                SUM(quantity_change) AS calculated_current_stock,
                MAX(event_timestamp) AS last_inventory_event
            FROM `data-engineering-b55.shopkart_silver.inventory_events`
            GROUP BY product_id
        )

        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_silver.products` p
        JOIN `data-engineering-b55.shopkart_gold.inventory_analytics` g
            ON p.product_id = g.product_id
        LEFT JOIN expected e
            ON p.product_id = e.product_id
        WHERE
            g.opening_stock IS DISTINCT FROM COALESCE(e.opening_stock, 0)
            OR g.units_sold IS DISTINCT FROM COALESCE(e.units_sold, 0)
            OR g.units_returned IS DISTINCT FROM COALESCE(e.units_returned, 0)
            OR g.units_restocked IS DISTINCT FROM COALESCE(e.units_restocked, 0)
            OR g.adjustment_net IS DISTINCT FROM COALESCE(e.adjustment_net, 0)
            OR g.total_inventory_events IS DISTINCT FROM
                COALESCE(e.total_inventory_events, 0)
            OR g.calculated_current_stock IS DISTINCT FROM
                COALESCE(e.calculated_current_stock, 0)
            OR g.last_inventory_event IS DISTINCT FROM
                e.last_inventory_event
    """,

    "Negative inventory stock": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_gold.inventory_analytics`
        WHERE calculated_current_stock < 0
    """,

    "Inventory event count reconciliation": """
        SELECT
            IF(
                (SELECT COUNT(*)
                 FROM `data-engineering-b55.shopkart_silver.inventory_events`)
                =
                (SELECT SUM(total_inventory_events)
                 FROM `data-engineering-b55.shopkart_gold.inventory_analytics`),
                0,
                1
            ) AS invalid_count
    """,

    # =====================================================
    # CROSS-TABLE BUSINESS RECONCILIATION
    # =====================================================

    "Gold revenue reconciliation": """
        SELECT
            IF(
                ABS(
                    (SELECT SUM(total_revenue)
                     FROM `data-engineering-b55.shopkart_gold.daily_sales_summary`)
                    -
                    (SELECT SUM(total_revenue)
                     FROM `data-engineering-b55.shopkart_gold.product_performance`)
                ) < 0.01
                AND
                ABS(
                    (SELECT SUM(total_revenue)
                     FROM `data-engineering-b55.shopkart_gold.daily_sales_summary`)
                    -
                    (SELECT SUM(lifetime_revenue)
                     FROM `data-engineering-b55.shopkart_gold.customer_analytics`)
                ) < 0.01,
                0,
                1
            ) AS invalid_count
    """,

    "Gold units sold reconciliation": """
        SELECT
            IF(
                (SELECT SUM(total_units_sold)
                 FROM `data-engineering-b55.shopkart_gold.daily_sales_summary`)
                =
                (SELECT SUM(total_units_sold)
                 FROM `data-engineering-b55.shopkart_gold.product_performance`),
                0,
                1
            ) AS invalid_count
    """,

    # =====================================================
    # LOAD TIMESTAMP CHECKS
    # =====================================================

    "Daily sales load timestamp": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_gold.daily_sales_summary`
        WHERE gold_loaded_at IS NULL
    """,

    "Product performance load timestamp": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_gold.product_performance`
        WHERE gold_loaded_at IS NULL
    """,

    "Customer analytics load timestamp": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_gold.customer_analytics`
        WHERE gold_loaded_at IS NULL
    """,

    "Payment analytics load timestamp": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_gold.payment_analytics`
        WHERE gold_loaded_at IS NULL
    """,

    "Inventory analytics load timestamp": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_gold.inventory_analytics`
        WHERE gold_loaded_at IS NULL
    """,
}


# =====================================================
# RUN GOLD DATA QUALITY VALIDATION
# =====================================================

def validate_gold():

    passed = 0
    failed = 0
    errors = 0

    print("\n========== GOLD DATA QUALITY VALIDATION ==========\n")

    for check_name, query in GOLD_CHECKS.items():

        try:
            result = list(client.query(query).result())

            invalid_count = result[0]["invalid_count"]

            if invalid_count == 0:
                print(f"PASS | {check_name}")
                passed += 1

            else:
                print(
                    f"FAIL | {check_name} | "
                    f"Invalid records: {invalid_count}"
                )
                failed += 1

        except Exception as error:
            print(f"ERROR | {check_name} | {error}")
            errors += 1

    total = len(GOLD_CHECKS)

    print("\n========== DATA QUALITY SUMMARY ==========")
    print(f"Total checks : {total}")
    print(f"Passed       : {passed}")
    print(f"Failed       : {failed}")
    print(f"Errors       : {errors}")

    if failed == 0 and errors == 0:
        print("\nGold data quality validation successful!")

    else:
        print("\nGold data quality validation failed!")


if __name__ == "__main__":
    validate_gold()
