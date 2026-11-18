
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker


# --------------------------------------------------
# 1. Configuration
# --------------------------------------------------

TOTAL_CUSTOMERS = 1000
RANDOM_SEED = 42

fake = Faker("en_IN")
Faker.seed(RANDOM_SEED)

rng = np.random.default_rng(RANDOM_SEED)


# --------------------------------------------------
# 2. Customer business data
# --------------------------------------------------

locations = [
    ("Hyderabad", "Telangana", "500"),
    ("Bengaluru", "Karnataka", "560"),
    ("Chennai", "Tamil Nadu", "600"),
    ("Mumbai", "Maharashtra", "400"),
    ("Pune", "Maharashtra", "411"),
    ("Delhi", "Delhi", "110"),
    ("Vijayawada", "Andhra Pradesh", "520"),
    ("Kochi", "Kerala", "682"),
    ("Kolkata", "West Bengal", "700"),
    ("Jaipur", "Rajasthan", "302"),
]

segments = ["Regular", "Premium", "VIP"]

segment_probabilities = [0.60, 0.30, 0.10]


# --------------------------------------------------
# 3. Generate customer records
# --------------------------------------------------

def generate_customers():

    customers = []

    today = datetime.now()
    start_date = today - timedelta(days=730)

    for customer_number in range(1, TOTAL_CUSTOMERS + 1):

        city, state, pin_prefix = locations[
            rng.integers(0, len(locations))
        ]

        registration_date = fake.date_between_dates(
            date_start=start_date.date(),
            date_end=today.date()
        )

        created_at = datetime.combine(
            registration_date,
            fake.time_object()
        )

        updated_at = created_at + timedelta(
            days=int(rng.integers(0, 31))
        )

        if updated_at > today:
            updated_at = today

        customer = {
            "customer_id": customer_number,
            "customer_name": fake.name(),
            "email": f"customer{customer_number}@shopkart.example",
            "phone": f"+91{rng.integers(6000000000, 10000000000)}",
            "city": city,
            "state": state,
            "pincode": (
                pin_prefix
                + f"{rng.integers(0, 1000):03d}"
            ),
            "registration_date": registration_date,
            "customer_segment": rng.choice(
                segments,
                p=segment_probabilities
            ),
            "is_active": bool(rng.choice([True, False], p=[0.90, 0.10])),
            "created_at": created_at,
            "updated_at": updated_at
        }

        customers.append(customer)

    return pd.DataFrame(customers)


# --------------------------------------------------
# 4. Save customer data
# --------------------------------------------------

def main():

    project_root = Path(__file__).resolve().parents[2]

    output_folder = project_root / "data" / "raw"
    output_folder.mkdir(parents=True, exist_ok=True)

    output_file = output_folder / "customers.csv"

    df = generate_customers()

    df.to_csv(output_file, index=False)

    print("Customer data generation completed!")
    print(f"Total customers: {len(df)}")
    print(f"Output file: {output_file}")


if __name__ == "__main__":
    main()
