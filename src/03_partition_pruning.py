"""
Partition Pruning
-----------------
Naive: query a non-partitioned table with a date filter — Spark has to
scan every file to find matching rows.

Optimized: write the table partitioned by sale_date. A query filtering on
sale_date now skips reading partitions that can't match — visible as
`PartitionFilters` in the physical plan instead of a full scan.
"""
from spark_session import get_spark

ORDERS_PATH = "data/raw/orders.csv"
UNPARTITIONED_PATH = "data/unpartitioned/orders"
PARTITIONED_PATH = "data/partitioned/orders"
FILTER_DATE = "2026-07-15"


def load_orders(spark):
    return spark.read.option("header", True).option("inferSchema", True).csv(ORDERS_PATH)


def write_unpartitioned(orders):
    orders.write.mode("overwrite").parquet(UNPARTITIONED_PATH)


def write_partitioned(orders):
    orders.write.mode("overwrite").partitionBy("sale_date").parquet(PARTITIONED_PATH)


def main():
    spark = get_spark()
    orders = load_orders(spark)

    write_unpartitioned(orders)
    write_partitioned(orders)

    print("=== Query plan: non-partitioned table (full scan) ===")
    unpartitioned_df = spark.read.parquet(UNPARTITIONED_PATH)
    unpartitioned_filtered = unpartitioned_df.filter(unpartitioned_df.sale_date == FILTER_DATE)
    unpartitioned_filtered.explain()

    print("\n=== Query plan: partitioned table (pruned) ===")
    partitioned_df = spark.read.parquet(PARTITIONED_PATH)
    partitioned_filtered = partitioned_df.filter(partitioned_df.sale_date == FILTER_DATE)
    partitioned_filtered.explain()
    # Look for "PartitionFilters" in this plan vs. "PushedFilters"-only above —
    # PartitionFilters means whole files/directories were skipped, not just
    # rows filtered after being read.

    print(f"\nRow counts match: {unpartitioned_filtered.count() == partitioned_filtered.count()}")


if __name__ == "__main__":
    main()
