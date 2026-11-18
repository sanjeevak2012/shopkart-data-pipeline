import pandas as pd

inventory = pd.read_csv("data/raw/inventory_events.csv")
products = pd.read_csv("data/raw/products.csv")
orders = pd.read_csv("data/raw/orders.csv")
order_items = pd.read_csv("data/raw/order_items.csv")

inventory["event_timestamp"] = pd.to_datetime(
    inventory["event_timestamp"]
)

print("Inventory shape:", inventory.shape)
print(inventory.head())
# Duplicate event IDs
print(
    "Duplicate event IDs:",
    inventory["event_id"].duplicated().sum()
)

# Missing values
print("\nMissing values:")
print(inventory.isnull().sum())

# Invalid product references
print(
    "\nInvalid product references:",
    (~inventory["product_id"].isin(products["product_id"])).sum()
)

# Invalid quantity signs
invalid_signs = inventory[
    (
        inventory["event_type"].isin(
            ["OPENING_BALANCE", "RESTOCK", "RETURN"]
        ) &
        (inventory["quantity_change"] <= 0)
    ) |
    (
        inventory["event_type"] == "SALE"
    ) &
    (inventory["quantity_change"] >= 0)
]

print("Invalid quantity signs:", len(invalid_signs))
eligible_orders = orders[
    orders["order_status"].isin(["Shipped", "Delivered"])
]

expected_sales = order_items.merge(
    eligible_orders[["order_id"]],
    on="order_id",
    how="inner"
)

actual_sales = inventory[
    inventory["event_type"] == "SALE"
]

print("Expected sale events:", len(expected_sales))
print("Actual sale events:", len(actual_sales))
inventory = inventory.sort_values(
    ["product_id", "event_timestamp", "event_id"]
)

inventory["running_stock"] = (
    inventory.groupby("product_id")["quantity_change"]
    .cumsum()
)

negative_stock = inventory[
    inventory["running_stock"] < 0
]

print("Negative stock events:", len(negative_stock))
