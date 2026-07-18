"""
Adaptive Query Execution (AQE)
--------------------------------
AQE (spark.sql.adaptive.enabled, on by default since Spark 3.2) re-optimizes
the query plan mid-execution using actual runtime statistics instead of
relying solely on static pre-execution estimates. This script shows the
same query running with AQE off vs. on so the plan/behavior difference is
visible.

Key AQE features demonstrated:
  - Coalescing post-shuffle partitions automatically
  - Dynamically switching a SortMergeJoin to a BroadcastHashJoin at runtime
    if actual stats show one side is small enough
"""
from spark_session import get_spark

ORDERS_PATH = "data/raw/orders.csv"
PRODUCTS_PATH = "data/raw/products.csv"


def load_data(spark):
    orders = spark.read.option("header", True).option("inferSchema", True).csv(ORDERS_PATH)
    products = spark.read.option("header", True).option("inferSchema", True).csv(PRODUCTS_PATH)
    return orders, products


def run_join(orders, products):
    return orders.filter("quantity > 3").join(products, on="product_id")


def main():
    spark = get_spark()
    orders, products = load_data(spark)

    print("=== AQE disabled ===")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    result_off = run_join(orders, products)
    result_off.explain()
    # Plan is fixed at compile time based on static/estimated statistics —
    # note the shuffle partition count matches spark.sql.shuffle.partitions
    # exactly (default 200), regardless of actual data size.

    print("\n=== AQE enabled ===")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    result_on = run_join(orders, products)
    result_on.explain()
    # With AQE, Spark can coalesce many small post-shuffle partitions into
    # fewer, right-sized ones after seeing actual shuffle output sizes, and
    # can switch join strategy at runtime if one side turns out small.

    print(f"\nRow counts match: {result_off.count() == result_on.count()}")


if __name__ == "__main__":
    main()
