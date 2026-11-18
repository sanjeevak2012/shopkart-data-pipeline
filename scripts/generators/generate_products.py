
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker


# 1. Configuration

TOTAL_PRODUCTS = 200
RANDOM_SEED = 42

fake = Faker()
Faker.seed(RANDOM_SEED)

rng = np.random.default_rng(RANDOM_SEED)


# 2. Product categories

categories = {
    "Electronics": ["Mobile", "Laptop", "Headphones", "Tablet"],
    "Fashion": ["Shirt", "Jeans", "Shoes", "Jacket"],
    "Home": ["Furniture", "Cookware", "Decor", "Lighting"],
    "Beauty": ["Skincare", "Perfume", "Makeup"],
    "Books": ["Fiction", "Education", "Technology"],
    "Sports": ["Fitness", "Cricket", "Football"],
    "Grocery": ["Snacks", "Beverages", "Staples"],
    "Toys": ["Educational Toys", "Games", "Action Figures"],
    "Automotive": ["Accessories", "Car Care"],
    "Appliances": ["Mixer", "Refrigerator", "Washing Machine"]
}


# 3. Generate product records

def generate_products():

    products = []

    today = datetime.now()
    start_date = today - timedelta(days=730)

    for product_id in range(1, TOTAL_PRODUCTS + 1):

        category = rng.choice(list(categories.keys()))

        subcategory = rng.choice(categories[category])

        created_date = fake.date_between_dates(
            date_start=start_date.date(),
            date_end=today.date()
        )

        created_at = datetime.combine(
            created_date,
            fake.time_object()
        )

        updated_at = created_at + timedelta(
            days=int(rng.integers(0, 60))
        )

        if updated_at > today:
            updated_at = today

        product = {
            "product_id": product_id,
            "product_name": f"{fake.word().title()} {subcategory}",
            "category": category,
            "subcategory": subcategory,
            "price": round(
                float(rng.uniform(99, 75000)), 2
            ),
            "stock_quantity": int(
                rng.integers(0, 1000)
            ),
            "product_status": rng.choice(
                ["Active", "Inactive", "Discontinued"],
                p=[0.80, 0.15, 0.05]
            ),
            "created_at": created_at,
            "updated_at": updated_at
        }

        products.append(product)

    return pd.DataFrame(products)


# 4. Save product data

def main():

    project_root = Path(__file__).resolve().parents[2]

    output_folder = project_root / "data" / "raw"

    output_folder.mkdir(parents=True, exist_ok=True)

    output_file = output_folder / "products.csv"

    df = generate_products()

    df.to_csv(output_file, index=False)

    print("Product data generation completed!")
    print("Total products:", len(df))
    print("Total categories:", df["category"].nunique())
    print("Output file:", output_file)


if __name__ == "__main__":
    main()
