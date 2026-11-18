
import pandas as pd

df = pd.read_csv("data/raw/customers.csv")

# Convert columns to appropriate types
df["phone"] = df["phone"].astype("string")
df["pincode"] = df["pincode"].astype("string")

df["registration_date"] = pd.to_datetime(
    df["registration_date"],
    errors="coerce"
)

df["created_at"] = pd.to_datetime(
    df["created_at"],
    errors="coerce"
)

df["updated_at"] = pd.to_datetime(
    df["updated_at"],
    errors="coerce"
)

# Data quality checks
print("Total records:", len(df))
print("Duplicate customer IDs:", df["customer_id"].duplicated().sum())
print("Duplicate emails:", df["email"].duplicated().sum())
print("Missing values:\n", df.isnull().sum())

print("\nCorrected data types:")
print(df.dtypes)
