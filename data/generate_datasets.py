"""
Generates the datasets used across every optimization example:
- A large "orders" fact table
- A small "products" dimension table (for broadcast join)
- A skewed "orders_skewed" table (one customer_id dominates, for skew handling)
"""
import os
import random
import csv
import uuid

os.makedirs("data/raw", exist_ok=True)

NUM_ORDERS = 200_000
NUM_PRODUCTS = 200
NUM_CUSTOMERS = 5000

REGIONS = ["North", "South", "East", "West"]


def generate_products():
    path = "data/raw/products.csv"
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "product_name", "category", "unit_price"])
        for i in range(NUM_PRODUCTS):
            writer.writerow([f"P{i:04d}", f"Product {i}", random.choice(["Electronics", "Grocery", "Apparel"]),
                              round(random.uniform(5, 500), 2)])
    print(f"Wrote {NUM_PRODUCTS} products to {path}")


def generate_orders():
    path = "data/raw/orders.csv"
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "customer_id", "product_id", "region", "quantity", "sale_date"])
        for i in range(NUM_ORDERS):
            customer_id = f"C{random.randint(0, NUM_CUSTOMERS - 1):05d}"
            product_id = f"P{random.randint(0, NUM_PRODUCTS - 1):04d}"
            date = f"2026-{random.randint(1,7):02d}-{random.randint(1,28):02d}"
            writer.writerow([str(uuid.uuid4()), customer_id, product_id,
                              random.choice(REGIONS), random.randint(1, 10), date])
    print(f"Wrote {NUM_ORDERS} orders to {path}")


def generate_skewed_orders():
    """95% of rows share a single customer_id — simulates a real skew scenario
    like one whale account or one bot-driven customer dominating a table."""
    path = "data/raw/orders_skewed.csv"
    hot_customer = "C_HOT_0001"
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "customer_id", "product_id", "region", "quantity", "sale_date"])
        for i in range(NUM_ORDERS):
            if random.random() < 0.95:
                customer_id = hot_customer
            else:
                customer_id = f"C{random.randint(0, NUM_CUSTOMERS - 1):05d}"
            product_id = f"P{random.randint(0, NUM_PRODUCTS - 1):04d}"
            date = f"2026-{random.randint(1,7):02d}-{random.randint(1,28):02d}"
            writer.writerow([str(uuid.uuid4()), customer_id, product_id,
                              random.choice(REGIONS), random.randint(1, 10), date])
    print(f"Wrote {NUM_ORDERS} skewed orders (hot key: {hot_customer}) to {path}")


if __name__ == "__main__":
    generate_products()
    generate_orders()
    generate_skewed_orders()
