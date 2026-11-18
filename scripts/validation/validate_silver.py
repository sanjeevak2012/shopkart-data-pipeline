
from google.cloud import bigquery

# --------------------------------------------------
# 1. BIGQUERY CONNECTION
# --------------------------------------------------

client = bigquery.Client(project="data-engineering-b55")
LOCATION = "asia-south1"


# --------------------------------------------------
# 2. SILVER DATA QUALITY CHECKS
# --------------------------------------------------

checks = {

    # EXPECTED ROW COUNTS

    "Customers row count": """
        SELECT IF(COUNT(*) = 1000, 0, 1) AS invalid_count
        FROM `data-engineering-b55.shopkart_silver.customers`
    """,

    "Products row count": """
        SELECT IF(COUNT(*) = 200, 0, 1) AS invalid_count
        FROM `data-engineering-b55.shopkart_silver.products`
    """,

    "Orders row count": """
        SELECT IF(COUNT(*) = 5000, 0, 1) AS invalid_count
        FROM `data-engineering-b55.shopkart_silver.orders`
    """,

    "Order items row count": """
        SELECT IF(COUNT(*) = 9980, 0, 1) AS invalid_count
        FROM `data-engineering-b55.shopkart_silver.order_items`
    """,

    "Payments row count": """
        SELECT IF(COUNT(*) = 5684, 0, 1) AS invalid_count
        FROM `data-engineering-b55.shopkart_silver.payments`
    """,

    "Inventory events row count": """
        SELECT IF(COUNT(*) = 7572, 0, 1) AS invalid_count
        FROM `data-engineering-b55.shopkart_silver.inventory_events`
    """,

    # PRIMARY KEY UNIQUENESS

    "Duplicate customer IDs": """
        SELECT COUNT(*) AS invalid_count
        FROM (
            SELECT customer_id
            FROM `data-engineering-b55.shopkart_silver.customers`
            GROUP BY customer_id
            HAVING COUNT(*) > 1
        )
    """,

    "Duplicate product IDs": """
        SELECT COUNT(*) AS invalid_count
        FROM (
            SELECT product_id
            FROM `data-engineering-b55.shopkart_silver.products`
            GROUP BY product_id
            HAVING COUNT(*) > 1
        )
    """,

    "Duplicate order IDs": """
        SELECT COUNT(*) AS invalid_count
        FROM (
            SELECT order_id
            FROM `data-engineering-b55.shopkart_silver.orders`
            GROUP BY order_id
            HAVING COUNT(*) > 1
        )
    """,

    "Duplicate payment IDs": """
        SELECT COUNT(*) AS invalid_count
        FROM (
            SELECT payment_id
            FROM `data-engineering-b55.shopkart_silver.payments`
            GROUP BY payment_id
            HAVING COUNT(*) > 1
        )
    """,

    "Duplicate inventory event IDs": """
        SELECT COUNT(*) AS invalid_count
        FROM (
            SELECT event_id
            FROM `data-engineering-b55.shopkart_silver.inventory_events`
            GROUP BY event_id
            HAVING COUNT(*) > 1
        )
    """,

    "Duplicate order-product combinations": """
        SELECT COUNT(*) AS invalid_count
        FROM (
            SELECT order_id, product_id
            FROM `data-engineering-b55.shopkart_silver.order_items`
            GROUP BY order_id, product_id
            HAVING COUNT(*) > 1
        )
    """,

    # FOREIGN KEY RELATIONSHIPS

    "Invalid order customer references": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_silver.orders` o
        LEFT JOIN `data-engineering-b55.shopkart_silver.customers` c
        ON o.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """,

    "Invalid order item order references": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_silver.order_items` oi
        LEFT JOIN `data-engineering-b55.shopkart_silver.orders` o
        ON oi.order_id = o.order_id
        WHERE o.order_id IS NULL
    """,

    "Invalid order item product references": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_silver.order_items` oi
        LEFT JOIN `data-engineering-b55.shopkart_silver.products` p
        ON oi.product_id = p.product_id
        WHERE p.product_id IS NULL
    """,

    "Invalid payment order references": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_silver.payments` p
        LEFT JOIN `data-engineering-b55.shopkart_silver.orders` o
        ON p.order_id = o.order_id
        WHERE o.order_id IS NULL
    """,

    "Invalid inventory product references": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_silver.inventory_events` i
        LEFT JOIN `data-engineering-b55.shopkart_silver.products` p
        ON i.product_id = p.product_id
        WHERE p.product_id IS NULL
    """,

    # BUSINESS RULES

    "Order total matches order items": """
        SELECT COUNT(*) AS invalid_count
        FROM (
            SELECT
                o.order_id
            FROM `data-engineering-b55.shopkart_silver.orders` o
            LEFT JOIN `data-engineering-b55.shopkart_silver.order_items` oi
            ON o.order_id = oi.order_id
            GROUP BY o.order_id, o.order_total
            HAVING COUNT(oi.order_id) = 0
                OR o.order_total != SUM(oi.line_total)
        )
    """,

    "Order item financial calculations": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_silver.order_items`
        WHERE quantity <= 0
           OR unit_price <= 0
           OR line_total != quantity * unit_price
    """,

    "Payment date and status rules": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_silver.payments`
        WHERE
            (payment_status = 'PENDING'
             AND payment_date IS NOT NULL)
            OR
            (payment_status IN ('SUCCESS', 'FAILED')
             AND payment_date IS NULL)
    """,

    "Payment attempt sequence": """
        SELECT COUNT(*) AS invalid_count
        FROM (
            SELECT order_id
            FROM `data-engineering-b55.shopkart_silver.payments`
            GROUP BY order_id
            HAVING MIN(attempt_number) != 1
                OR COUNT(*) != COUNT(DISTINCT attempt_number)
                OR MAX(attempt_number) != COUNT(*)
                OR MAX(attempt_number) > 3
        )
    """,

    "Inventory quantity movement rules": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_silver.inventory_events`
        WHERE
            (event_type = 'OPENING_BALANCE' AND quantity_change <= 0)
            OR (event_type IN ('RESTOCK', 'RETURN') AND quantity_change <= 0)
            OR (event_type = 'SALE' AND quantity_change >= 0)
            OR (event_type = 'ADJUSTMENT' AND quantity_change = 0)
    """
}


# --------------------------------------------------
# 3. EXECUTE CHECKS
# --------------------------------------------------

results = []

print("\n========== SILVER DATA QUALITY CHECKS ==========\n")

for check_name, query in checks.items():

    try:
        query_job = client.query(
            query,
            location=LOCATION
        )

        rows = list(query_job.result())
        invalid_count = int(rows[0]["invalid_count"])

        status = "PASS" if invalid_count == 0 else "FAIL"

        results.append({
            "check": check_name,
            "invalid_records": invalid_count,
            "status": status
        })

        print(
            f"{status} | {check_name} | "
            f"Invalid records: {invalid_count}"
        )

    except Exception as error:

        results.append({
            "check": check_name,
            "invalid_records": None,
            "status": "ERROR"
        })

        print(f"ERROR | {check_name}")
        print(f"Reason: {error}")


# --------------------------------------------------
# 4. FINAL SUMMARY
# --------------------------------------------------

passed = sum(
    1 for result in results
    if result["status"] == "PASS"
)

failed = sum(
    1 for result in results
    if result["status"] == "FAIL"
)

errors = sum(
    1 for result in results
    if result["status"] == "ERROR"
)

print("\n========== SILVER DATA QUALITY SUMMARY ==========")

print(f"Total checks : {len(results)}")
print(f"Passed       : {passed}")
print(f"Failed       : {failed}")
print(f"Errors       : {errors}")

if failed > 0 or errors > 0:
    raise SystemExit("Silver data quality validation failed!")

print("\nAll Silver data quality checks passed!")
