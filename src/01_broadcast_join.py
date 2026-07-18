"""
Broadcast Join
--------------
Naive: join a large orders table against a small products table using a
standard SortMergeJoin — both sides get shuffled across the cluster.

Optimized: broadcast the small products table so it's sent once to every
executor, eliminating the shuffle entirely.
"""
from pyspark.sql.functions import broadcast

from spark_session import get_spark

ORDERS_PATH = "data/raw/orders.csv"
PRODUCTS_PATH = "data/raw/products.csv"


def load_data(spark):
    orders = spark.read.option("header", True).option("inferSchema", True).csv(ORDERS_PATH)
    products = spark.read.option("header", True).option("inferSchema", True).csv(PRODUCTS_PATH)
    return orders, products


def naive_join(orders, products):
    """Forces a shuffle join by disabling auto-broadcast for this comparison."""
    return orders.join(products, on="product_id", how="inner")


def optimized_broadcast_join(orders, products):
    """Explicit broadcast hint — Spark ships `products` to every executor
    instead of shuffling both sides."""
    return orders.join(broadcast(products), on="product_id", how="inner")


def main():
    spark = get_spark()
    orders, products = load_data(spark)

    print("=== Naive join physical plan ===")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # force shuffle join for comparison
    naive_result = naive_join(orders, products)
    naive_result.explain()

    print("\n=== Broadcast join physical plan ===")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # restore default (10MB)
    optimized_result = optimized_broadcast_join(orders, products)
    optimized_result.explain()

    print(f"\nRow counts match: {naive_result.count() == optimized_result.count()}")


if __name__ == "__main__":
    main()
