
from pathlib import Path
import subprocess
import sys


# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

GENERATORS = [
    "generate_customers.py",
    "generate_products.py",
    "generate_orders.py",
    "generate_payments.py",
    "generate_inventory.py"
]


def main():

    print("=" * 55)
    print("SHOPKART DATA GENERATION STARTED")
    print("=" * 55)

    for generator in GENERATORS:

        script_path = (
            PROJECT_ROOT / "scripts" / "generators" / generator
        )

        print(f"\nRunning: {generator}")

        subprocess.run(
            [sys.executable, str(script_path)],
            cwd=PROJECT_ROOT,
            check=True
        )

        print(f"Completed: {generator}")

    print("\n" + "=" * 55)
    print("ALL DATASETS GENERATED SUCCESSFULLY")
    print("=" * 55)


if __name__ == "__main__":
    main()
