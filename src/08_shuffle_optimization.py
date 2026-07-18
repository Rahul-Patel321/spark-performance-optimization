"""
Shuffle Optimization (general principles)
--------------------------------------------
Demonstrates three shuffle-reduction techniques together on the same task
(computing total revenue-equivalent quantity per region):

  1. Filter/select before the wide transformation, not after —
     shuffle only the data you actually need.
  2. Prefer combiner-side aggregation (reduceByKey-style) over shuffling
     all raw rows before aggregating (groupByKey-style).
  3. Tune spark.sql.shuffle.partitions — the default (200) is frequently
     wrong for both small and very large datasets.
"""
from pyspark.sql import functions as F
from spark_session import get_spark

ORDERS_PATH = "data/raw/orders.csv"


def naive_approach(spark, orders):
    """Selects all columns, shuffles everything, THEN filters and aggregates.
    Wastes shuffle I/O moving columns and rows that get discarded anyway."""
    spark.conf.set("spark.sql.shuffle.partitions", "200")  # default, likely oversized here

    return (
        orders  # no column pruning before the shuffle
        .groupBy("region", "customer_id")  # shuffles on a needlessly fine grain
        .agg(F.sum("quantity").alias("total_quantity"))
        .filter("total_quantity > 5")
        .groupBy("region")
        .agg(F.sum("total_quantity").alias("region_total"))
    )


def optimized_approach(spark, orders):
    """Prunes columns and rows before the shuffle, aggregates directly at
    the grain that's actually needed, and sizes shuffle partitions to the
    dataset instead of leaving the default."""
    # Right-size shuffle partitions — 200 is overkill for a dataset this
    # small locally; in production you'd size this to input data volume
    # (roughly 100-200MB per output partition is a reasonable target).
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    return (
        orders.select("region", "quantity")  # prune columns before shuffling
        .groupBy("region")                    # aggregate directly at the needed grain
        .agg(F.sum("quantity").alias("region_total"))
    )


def main():
    spark = get_spark()
    orders = spark.read.option("header", True).option("inferSchema", True).csv(ORDERS_PATH)

    print("=== Naive shuffle plan ===")
    naive_result = naive_approach(spark, orders)
    naive_result.explain()

    print("\n=== Optimized shuffle plan ===")
    optimized_result = optimized_approach(spark, orders)
    optimized_result.explain()

    naive_totals = {r["region"]: r["region_total"] for r in naive_result.collect()}
    optimized_totals = {r["region"]: r["region_total"] for r in optimized_result.collect()}
    print(f"\nResults match: {naive_totals == optimized_totals}")


if __name__ == "__main__":
    main()
