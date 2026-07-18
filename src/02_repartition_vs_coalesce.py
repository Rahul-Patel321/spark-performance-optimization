"""
Repartition vs. Coalesce
-------------------------
repartition(n): full shuffle, can go up or down in partition count, produces
evenly balanced partitions. Use to increase parallelism or fix skew.

coalesce(n): no shuffle, can only reduce partition count, merges adjacent
partitions without redistributing rows. Use to reduce output files cheaply
before a write when even distribution doesn't matter.
"""
from spark_session import get_spark

ORDERS_PATH = "data/raw/orders.csv"


def load_orders(spark):
    return spark.read.option("header", True).option("inferSchema", True).csv(ORDERS_PATH)


def show_partition_distribution(df, label):
    sizes = df.rdd.glom().map(len).collect()
    print(f"{label}: {len(sizes)} partitions, row counts per partition: "
          f"min={min(sizes)}, max={max(sizes)}, avg={sum(sizes)/len(sizes):.0f}")


def main():
    spark = get_spark()
    orders = load_orders(spark)

    print(f"Initial partition count: {orders.rdd.getNumPartitions()}")
    show_partition_distribution(orders, "Original")

    # coalesce: cheap, no shuffle, but distribution can be very uneven
    # since it just merges existing partitions together.
    coalesced = orders.coalesce(4)
    show_partition_distribution(coalesced, "Coalesced to 4")

    # repartition: triggers a full shuffle, but partitions come out balanced.
    repartitioned = orders.repartition(4)
    show_partition_distribution(repartitioned, "Repartitioned to 4")

    # repartition on a column is the version that matters most in practice —
    # it groups all rows for a key onto the same partition, which is exactly
    # what you want before a groupBy/join on that column.
    repartitioned_by_key = orders.repartition(8, "region")
    show_partition_distribution(repartitioned_by_key, "Repartitioned by 'region' to 8")


if __name__ == "__main__":
    main()
