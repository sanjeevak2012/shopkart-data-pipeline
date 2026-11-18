
from pathlib import Path

import pandas as pd
import numpy as np


# --------------------------------------------------
# 1. Project paths and configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

PRODUCTS_FILE = PROJECT_ROOT / "data" / "raw" / "products.csv"
ORDERS_FILE = PROJECT_ROOT / "data" / "raw" / "orders.csv"
ORDER_ITEMS_FILE = PROJECT_ROOT / "data" / "raw" / "order_items.csv"

OUTPUT_FILE = PROJECT_ROOT / "data" / "raw" / "inventory_events.csv"

rng = np.random.default_rng(42)


# --------------------------------------------------
# 2. Read source datasets
# --------------------------------------------------

products = pd.read_csv(PRODUCTS_FILE)
orders = pd.read_csv(ORDERS_FILE)
order_items = pd.read_csv(ORDER_ITEMS_FILE)

orders["order_date"] = pd.to_datetime(orders["order_date"])

sales = order_items.merge(
    orders[["order_id", "order_date", "order_status"]],
    on="order_id",
    how="inner"
)

# Deduct physical inventory only for shipped and delivered orders
sales = sales[
    sales["order_status"].isin(["Shipped", "Delivered"])
].copy()

events = []
event_id = 1

first_order_date = orders["order_date"].min()
baseline_date = first_order_date - pd.Timedelta(days=2)


# --------------------------------------------------
# 3. Generate inventory opening balances
# --------------------------------------------------

for _, product in products.iterrows():

    product_id = int(product["product_id"])
    opening_stock = int(product["stock_quantity"])

    events.append({
        "event_id": event_id,
        "product_id": product_id,
        "event_type": "OPENING_BALANCE",
        "quantity_change": opening_stock,
        "event_timestamp": baseline_date,
        "reference_id": f"PRODUCT_{product_id}",
        "reason": "Initial inventory balance",
        "created_at": baseline_date
    })

    event_id += 1


# --------------------------------------------------
# 4. Generate restock events
# --------------------------------------------------

total_sales = (
    sales.groupby("product_id")["quantity"]
    .sum()
)

for _, product in products.iterrows():

    product_id = int(product["product_id"])
    opening_stock = int(product["stock_quantity"])

    sold_quantity = int(total_sales.get(product_id, 0))

    # Maintain at least 50 units after generated sales
    restock_quantity = max(
        0,
        sold_quantity - opening_stock + 50
    )

    if restock_quantity > 0:

        restock_date = baseline_date + pd.Timedelta(days=1)

        events.append({
            "event_id": event_id,
            "product_id": product_id,
            "event_type": "RESTOCK",
            "quantity_change": restock_quantity,
            "event_timestamp": restock_date,
            "reference_id": f"RESTOCK_{product_id}",
            "reason": "Scheduled warehouse replenishment",
            "created_at": restock_date
        })

        event_id += 1


# --------------------------------------------------
# 5. Generate SALE events from actual order items
# --------------------------------------------------

for _, sale in sales.iterrows():

    product_id = int(sale["product_id"])
    order_id = int(sale["order_id"])
    quantity = int(sale["quantity"])
    sale_date = sale["order_date"]

    events.append({
        "event_id": event_id,
        "product_id": product_id,
        "event_type": "SALE",
        "quantity_change": -quantity,
        "event_timestamp": sale_date,
        "reference_id": f"ORDER_{order_id}",
        "reason": "Inventory deducted for customer order",
        "created_at": sale_date
    })

    event_id += 1


# --------------------------------------------------
# 6. Generate customer RETURN events
# --------------------------------------------------

eligible_sales = sales[
    (sales["order_status"] == "Delivered") &
    (
        sales["order_date"] <=
        pd.Timestamp.now() - pd.Timedelta(days=7)
    )
]

for _, sale in eligible_sales.iterrows():

    # Approximately 5% of eligible sale lines are returned
    if rng.random() < 0.05:

        returned_quantity = int(
            rng.integers(1, int(sale["quantity"]) + 1)
        )

        return_date = sale["order_date"] + pd.Timedelta(
            days=int(rng.integers(1, 8))
        )

        events.append({
            "event_id": event_id,
            "product_id": int(sale["product_id"]),
            "event_type": "RETURN",
            "quantity_change": returned_quantity,
            "event_timestamp": return_date,
            "reference_id": f"ORDER_{int(sale['order_id'])}",
            "reason": "Customer returned product",
            "created_at": return_date
        })

        event_id += 1


# --------------------------------------------------
# 7. Generate stock ADJUSTMENT events
# --------------------------------------------------

for _, product in products.iterrows():

    if rng.random() < 0.10:

        adjustment = int(rng.choice([-5, -3, -2, 2, 3, 5]))

        adjustment_date = pd.Timestamp.now()

        events.append({
            "event_id": event_id,
            "product_id": int(product["product_id"]),
            "event_type": "ADJUSTMENT",
            "quantity_change": adjustment,
            "event_timestamp": adjustment_date,
            "reference_id": f"ADJUSTMENT_{product['product_id']}",
            "reason": "Warehouse stock reconciliation",
            "created_at": adjustment_date
        })

        event_id += 1


# --------------------------------------------------
# 8. Save inventory events
# --------------------------------------------------

inventory_df = pd.DataFrame(events)

inventory_df = inventory_df.sort_values(
    ["event_timestamp", "event_id"]
).reset_index(drop=True)

inventory_df.to_csv(OUTPUT_FILE, index=False)


# --------------------------------------------------
# 9. Display results
# --------------------------------------------------

print("Inventory events generated successfully!")

print("Total inventory events:", len(inventory_df))

print("\nEvent distribution:")
print(inventory_df["event_type"].value_counts())

print("\nUnique products:", inventory_df["product_id"].nunique())

print("\nOutput file:", OUTPUT_FILE)
