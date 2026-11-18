
from google.cloud import bigquery

# --------------------------------------------------
# 1. BIGQUERY CONNECTION
# --------------------------------------------------

client = bigquery.Client(project="data-engineering-b55")

LOCATION = "asia-south1"


# --------------------------------------------------
# 2. DATA QUALITY CHECKS
# --------------------------------------------------

checks = {

    # CUSTOMERS

    "Duplicate customer IDs": """
        SELECT COUNT(*) AS invalid_count
        FROM (
            SELECT customer_id
            FROM `data-engineering-b55.shopkart_bronze.customers`
            GROUP BY customer_id
            HAVING COUNT(*) > 1
        )
    """,

    "Missing mandatory customer values": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_bronze.customers`
        WHERE customer_id IS NULL
           OR customer_name IS NULL
           OR email IS NULL
           OR registration_date IS NULL
    """,

    # PRODUCTS

    "Duplicate product IDs": """
        SELECT COUNT(*) AS invalid_count
        FROM (
            SELECT product_id
            FROM `data-engineering-b55.shopkart_bronze.products`
            GROUP BY product_id
            HAVING COUNT(*) > 1
        )
    """,

    "Invalid product prices or stock": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_bronze.products`
        WHERE price IS NULL OR price <= 0
           OR stock_quantity IS NULL OR stock_quantity < 0
    """,

    # ORDERS

    "Orders with invalid customers": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_bronze.orders` o
        LEFT JOIN `data-engineering-b55.shopkart_bronze.customers` c
        ON o.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """,

    # ORDER ITEMS

    "Order items with invalid orders": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_bronze.order_items` oi
        LEFT JOIN `data-engineering-b55.shopkart_bronze.orders` o
        ON oi.order_id = o.order_id
        WHERE o.order_id IS NULL
    """,

    "Order items with invalid products": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_bronze.order_items` oi
        LEFT JOIN `data-engineering-b55.shopkart_bronze.products` p
        ON oi.product_id = p.product_id
        WHERE p.product_id IS NULL
    """,

    "Mismatched order totals": """
        SELECT COUNT(*) AS invalid_count
        FROM (
            SELECT
                o.order_id,
                o.order_total,
                SUM(oi.line_total) AS calculated_total,
                COUNT(oi.order_id) AS item_count
            FROM `data-engineering-b55.shopkart_bronze.orders` o
            LEFT JOIN `data-engineering-b55.shopkart_bronze.order_items` oi
            ON o.order_id = oi.order_id
            GROUP BY o.order_id, o.order_total
            HAVING item_count = 0
                OR o.order_total != calculated_total
        )
    """,

    # PAYMENTS

    "Payments with invalid orders": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_bronze.payments` p
        LEFT JOIN `data-engineering-b55.shopkart_bronze.orders` o
        ON p.order_id = o.order_id
        WHERE o.order_id IS NULL
    """,

    "Invalid payment statuses": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_bronze.payments`
        WHERE payment_status IS NULL
           OR payment_status NOT IN ('SUCCESS', 'FAILED', 'PENDING')
    """,

    "Invalid payment amounts": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_bronze.payments`
        WHERE payment_amount IS NULL
           OR payment_amount <= 0
    """,

    "Invalid payment attempt groups": """
        SELECT COUNT(*) AS invalid_count
        FROM (
            SELECT order_id
            FROM `data-engineering-b55.shopkart_bronze.payments`
            GROUP BY order_id
            HAVING MIN(attempt_number) != 1
                OR COUNT(*) != COUNT(DISTINCT attempt_number)
                OR MAX(attempt_number) != COUNT(*)
                OR MAX(attempt_number) > 3
        )
    """,

    "Invalid payment dates": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_bronze.payments`
        WHERE
            (payment_status = 'PENDING'
             AND payment_date IS NOT NULL)
            OR
            (payment_status IN ('SUCCESS', 'FAILED')
             AND payment_date IS NULL)
    """,

    "Payment amount mismatches": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_bronze.payments` p
        JOIN `data-engineering-b55.shopkart_bronze.orders` o
        ON p.order_id = o.order_id
        WHERE p.payment_amount != o.order_total
    """,

    # INVENTORY EVENTS

    "Duplicate inventory event IDs": """
        SELECT COUNT(*) AS invalid_count
        FROM (
            SELECT event_id
            FROM `data-engineering-b55.shopkart_bronze.inventory_events`
            GROUP BY event_id
            HAVING COUNT(*) > 1
        )
    """,

    "Inventory events with invalid products": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_bronze.inventory_events` i
        LEFT JOIN `data-engineering-b55.shopkart_bronze.products` p
        ON i.product_id = p.product_id
        WHERE p.product_id IS NULL
    """,

    "Invalid inventory event types": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_bronze.inventory_events`
        WHERE event_type IS NULL
           OR event_type NOT IN (
               'OPENING_BALANCE',
               'RESTOCK',
               'SALE',
               'RETURN',
               'ADJUSTMENT'
           )
    """,

    "Invalid inventory quantity signs": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_bronze.inventory_events`
        WHERE
            (event_type = 'OPENING_BALANCE' AND quantity_change <= 0)
            OR (event_type IN ('RESTOCK', 'RETURN') AND quantity_change <= 0)
            OR (event_type = 'SALE' AND quantity_change >= 0)
            OR (event_type = 'ADJUSTMENT' AND quantity_change = 0)
    """,

    "Missing mandatory inventory values": """
        SELECT COUNT(*) AS invalid_count
        FROM `data-engineering-b55.shopkart_bronze.inventory_events`
        WHERE event_id IS NULL
           OR product_id IS NULL
           OR event_type IS NULL
           OR quantity_change IS NULL
           OR event_timestamp IS NULL
    """

      
}


# --------------------------------------------------
# 3. EXECUTE VALIDATION CHECKS
# --------------------------------------------------

results = []

print("\n========== BRONZE DATA QUALITY CHECKS ==========\n")

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

print("\n========== DATA QUALITY SUMMARY ==========")

print(f"Total checks : {len(results)}")
print(f"Passed       : {passed}")
print(f"Failed       : {failed}")
print(f"Errors       : {errors}")

if failed > 0 or errors > 0:
    raise SystemExit("Bronze data quality validation failed!")

print("\nAll Bronze data quality checks passed!")
