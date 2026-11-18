
from pathlib import Path

import pandas as pd
import numpy as np


# --------------------------------------------------
# 1. Project paths and configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

ORDERS_FILE = PROJECT_ROOT / "data" / "raw" / "orders.csv"
OUTPUT_FILE = PROJECT_ROOT / "data" / "raw" / "payments.csv"

rng = np.random.default_rng(42)


# --------------------------------------------------
# 2. Read orders
# --------------------------------------------------

orders = pd.read_csv(ORDERS_FILE)

orders["order_date"] = pd.to_datetime(orders["order_date"])

payment_methods = [
    "UPI",
    "Credit Card",
    "Debit Card",
    "Net Banking",
    "COD"
]

payment_method_probabilities = [
    0.40, 0.20, 0.15, 0.10, 0.15
]


# --------------------------------------------------
# 3. Generate payment transactions
# --------------------------------------------------

payments = []
payment_id = 1

for _, order in orders.iterrows():

    # Cancelled orders are excluded from payment generation
    if order["order_status"] == "Cancelled":
        continue

    order_id = int(order["order_id"])
    order_date = order["order_date"]
    order_amount = round(float(order["order_total"]), 2)
    order_status = order["order_status"]

    payment_method = rng.choice(
        payment_methods,
        p=payment_method_probabilities
    )

    # ----------------------------------------------
    # COD payment handling
    # ----------------------------------------------

    if payment_method == "COD":

        if order_status == "Delivered":
            status = "SUCCESS"
            payment_date = order_date + pd.Timedelta(
                hours=int(rng.integers(1, 25))
            )
        else:
            status = "PENDING"
            payment_date = pd.NaT

        created_at = order_date
        updated_at = payment_date if status == "SUCCESS" else created_at

        payments.append({
            "payment_id": payment_id,
            "order_id": order_id,
            "attempt_number": 1,
            "payment_date": payment_date,
            "payment_method": payment_method,
            "payment_amount": order_amount,
            "payment_status": status,
            "transaction_id": f"SKTXN{payment_id:08d}",
            "payment_provider": "Cash on Delivery",
            "created_at": created_at,
            "updated_at": updated_at
        })

        payment_id += 1
        continue

    # ----------------------------------------------
    # Online payment handling
    # ----------------------------------------------

    attempt_count = int(
        rng.choice([1, 2, 3], p=[0.75, 0.20, 0.05])
    )

    outcome = rng.choice(
        ["SUCCESS", "FAILED", "PENDING"],
        p=[0.90, 0.07, 0.03]
    )

    for attempt in range(1, attempt_count + 1):

        attempt_time = order_date + pd.Timedelta(
            minutes=int(rng.integers(1, 11))
        )

        if outcome == "SUCCESS" and attempt == attempt_count:
            status = "SUCCESS"
        elif outcome == "PENDING" and attempt == attempt_count:
            status = "PENDING"
        else:
            status = "FAILED"

        payment_date = (
            pd.NaT if status == "PENDING" else attempt_time
        )

        payments.append({
            "payment_id": payment_id,
            "order_id": order_id,
            "attempt_number": attempt,
            "payment_date": payment_date,
            "payment_method": payment_method,
            "payment_amount": order_amount,
            "payment_status": status,
            "transaction_id": f"SKTXN{payment_id:08d}",
            "payment_provider": rng.choice([
                "Razorpay",
                "PayU",
                "Cashfree"
            ]),
            "created_at": attempt_time,
            "updated_at": attempt_time
        })

        payment_id += 1


# --------------------------------------------------
# 4. Create DataFrame and save CSV
# --------------------------------------------------

payments_df = pd.DataFrame(payments)

payments_df.to_csv(OUTPUT_FILE, index=False)

print("Payment data generated successfully!")
print("Total payment records:", len(payments_df))
print("Unique orders with payments:", payments_df["order_id"].nunique())
print("Output file:", OUTPUT_FILE)
