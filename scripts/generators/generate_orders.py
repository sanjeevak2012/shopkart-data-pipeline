
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


# Configuration
TOTAL_ORDERS = 5000
RANDOM_SEED = 42

rng = np.random.default_rng(RANDOM_SEED)

ORDER_STATUSES = [
    "Pending",
    "Confirmed",
    "Shipped",
    "Delivered",
    "Cancelled"
]

STATUS_PROBABILITIES = [0.08, 0.12, 0.15, 0.55, 0.10]


def generate_orders(customers, products):

    orders = []
    order_items = []

    today = datetime.now()
    start_date = today - timedelta(days=365)

    customer_ids = customers["customer_id"].to_numpy()
    product_ids = products["product_id"].to_numpy()

    product_prices = products.set_index(
        "product_id"
    )["price"].to_dict()

    customer_registration = pd.to_datetime(
        customers.set_index("customer_id")["registration_date"]
    ).to_dict()

    for order_id in range(10001, 10001 + TOTAL_ORDERS):

        # Select an existing customer
        customer_id = int(rng.choice(customer_ids))

        # Customer cannot order before registration
        earliest_date = max(
            start_date,
            customer_registration[customer_id]
        )

        order_date = earliest_date + (
            today - earliest_date
        ) * float(rng.random())

        order_status = str(
            rng.choice(
                ORDER_STATUSES,
                p=STATUS_PROBABILITIES
            )
        )

        number_of_products = int(
            rng.integers(1, 4)
        )

        selected_products = rng.choice(
            product_ids,
            size=number_of_products,
            replace=False
        )

        order_quantity_total = 0
        order_total = 0.0

        for product_id in selected_products:

            product_id = int(product_id)

            quantity = int(rng.integers(1, 5))

            unit_price = round(
                float(product_prices[product_id]), 2
            )

            line_total = round(
                quantity * unit_price, 2
            )

            order_quantity_total += quantity
            order_total += line_total

            order_items.append({
                "order_id": order_id,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "line_total": line_total
            })

        orders.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "order_date": order_date,
            "order_status": order_status,
            "total_quantity": order_quantity_total,
            "order_total": round(order_total, 2)
        })

    return (
        pd.DataFrame(orders),
        pd.DataFrame(order_items)
    )


def main():

    project_root = Path(__file__).resolve().parents[2]

    raw_folder = project_root / "data" / "raw"

    customers = pd.read_csv(
        raw_folder / "customers.csv"
    )

    products = pd.read_csv(
        raw_folder / "products.csv"
    )

    orders, order_items = generate_orders(
        customers,
        products
    )

    orders.to_csv(
        raw_folder / "orders.csv",
        index=False
    )

    order_items.to_csv(
        raw_folder / "order_items.csv",
        index=False
    )

    print("Order generation completed!")
    print("Total orders:", len(orders))
    print("Total order items:", len(order_items))
    print("Orders file:", raw_folder / "orders.csv")
    print("Order items file:", raw_folder / "order_items.csv")


if __name__ == "__main__":
    main()
