"""
Skew Handling (Salting)
-------------------------
Naive: join a skewed orders table (one customer_id holds ~95% of rows)
against a customer dimension — the single task handling that hot key's
partition becomes a straggler while every other task finishes instantly.

Optimized: "salt" the join key by appending a random suffix on the skewed
side and exploding the dimension side across the same salt range, spreading
the hot key's rows across multiple partitions/tasks instead of one.

AQE alternative: spark.sql.adaptive.skewJoin.enabled does this automatically
at runtime in modern Spark — salting is what you'd hand-roll without it, and
understanding both shows you understand *why* the AQE version works.
"""
from pyspark.sql import functions as F
from spark_session import get_spark

SKEWED_ORDERS_PATH = "data/raw/orders_skewed.csv"
NUM_SALT_BUCKETS = 10


def load_data(spark):
    orders = spark.read.option("header", True).option("inferSchema", True).csv(SKEWED_ORDERS_PATH)
    customers = orders.select("customer_id").distinct().withColumn(
        "signup_region", F.lit("Unknown")
    )
    return orders, customers


def naive_skewed_join(orders, customers):
    """One task ends up processing ~95% of the data — a classic straggler."""
    return orders.join(customers, on="customer_id")


def salted_join(orders, customers):
    """Spreads the hot key across NUM_SALT_BUCKETS partitions by giving it
    a synthetic salt suffix on both sides, then exploding the dimension
    side to match every possible salt value."""
    salted_orders = orders.withColumn(
        "salt", (F.rand() * NUM_SALT_BUCKETS).cast("int")
    ).withColumn("salted_customer_id", F.concat_ws("_", "customer_id", "salt"))

    salt_range = [str(i) for i in range(NUM_SALT_BUCKETS)]
    salted_customers = (
        customers.withColumn("salt", F.explode(F.array([F.lit(s) for s in salt_range])))
        .withColumn("salted_customer_id", F.concat_ws("_", "customer_id", "salt"))
    )

    return (
        salted_orders.join(salted_customers, on="salted_customer_id")
        .select(salted_orders["*"])
    )


def main():
    spark = get_spark()
    orders, customers = load_data(spark)

    print("=== Naive join on skewed key ===")
    naive_result = naive_skewed_join(orders, customers)
    naive_result.explain()
    print(f"Row count: {naive_result.count()}")

    print("\n=== Salted join ===")
    salted_result = salted_join(orders, customers)
    salted_result.explain()
    print(f"Row count: {salted_result.count()}")

    print(f"\nRow counts match: {naive_result.count() == salted_result.count()}")
    print("\nNote: with AQE skew join handling enabled "
          "(spark.sql.adaptive.skewJoin.enabled=true, default in modern Spark), "
          "the naive join above would often be auto-optimized without needing "
          "this manual salting step.")


if __name__ == "__main__":
    main()
