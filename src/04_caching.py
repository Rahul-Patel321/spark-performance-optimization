"""
Caching / Persistence
----------------------
Naive: reuse a DataFrame with an expensive transformation lineage across
multiple actions — Spark recomputes the entire lineage every single time.

Optimized: cache() the DataFrame after the expensive step so subsequent
actions reuse the materialized result instead of recomputing from scratch.
"""
import time

from pyspark.sql import functions as F
from spark_session import get_spark

ORDERS_PATH = "data/raw/orders.csv"


def expensive_transform(orders):
    """Stands in for a costly step — a wide aggregation plus a UDF-like
    computation — the kind of thing you do NOT want to redo three times."""
    return (
        orders.withColumn("quantity_squared", F.col("quantity") ** 2)  # pretend this is expensive
        .groupBy("customer_id", "region")
        .agg(F.sum("quantity_squared").alias("weighted_quantity"))
    )


def run_without_cache(orders):
    transformed = expensive_transform(orders)
    start = time.time()
    count = transformed.count()          # action 1 — recomputes full lineage
    total = transformed.agg(F.sum("weighted_quantity")).collect()[0][0]  # action 2 — recomputes AGAIN
    elapsed = time.time() - start
    return count, total, elapsed


def run_with_cache(orders):
    transformed = expensive_transform(orders).cache()
    count = transformed.count()          # action 1 — materializes and caches
    start = time.time()
    total = transformed.agg(F.sum("weighted_quantity")).collect()[0][0]  # action 2 — reads from cache
    elapsed = time.time() - start
    transformed.unpersist()              # always clean up — an uncleared cache leaks executor memory
    return count, total, elapsed


def main():
    spark = get_spark()
    orders = spark.read.option("header", True).option("inferSchema", True).csv(ORDERS_PATH)

    count_a, total_a, time_a = run_without_cache(orders)
    print(f"Without cache — second action took {time_a:.2f}s")

    count_b, total_b, time_b = run_with_cache(orders)
    print(f"With cache    — second action took {time_b:.2f}s")

    print(f"\nResults match: {count_a == count_b and total_a == total_b}")


if __name__ == "__main__":
    main()
