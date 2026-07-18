"""
Bucketing
---------
Naive: join two tables on customer_id with no bucketing — every query
against them re-shuffles both sides to co-locate matching keys.

Optimized: pre-bucket both tables by customer_id at write time (same
column, same bucket count). Spark can then join them without a shuffle at
query time, because matching keys are already co-located on disk.

Note: bucketing requires tables registered in a metastore (saveAsTable),
not ad hoc DataFrame reads — that's what makes Spark trust the bucketing
metadata enough to skip the shuffle.
"""
from spark_session import get_spark

ORDERS_PATH = "data/raw/orders.csv"
NUM_BUCKETS = 8


def load_orders(spark):
    return spark.read.option("header", True).option("inferSchema", True).csv(ORDERS_PATH)


def write_unbucketed_table(spark, orders):
    orders.write.mode("overwrite").saveAsTable("orders_unbucketed")


def write_bucketed_tables(spark, orders):
    # Split into two tables on the same key just to demonstrate a bucketed
    # join — in practice these would be genuinely different tables (e.g.
    # orders and customer_profile) sharing a join key.
    half_a = orders.filter("quantity <= 5")
    half_b = orders.filter("quantity > 5")

    (
        half_a.write.mode("overwrite")
        .bucketBy(NUM_BUCKETS, "customer_id")
        .sortBy("customer_id")
        .saveAsTable("orders_bucketed_a")
    )
    (
        half_b.write.mode("overwrite")
        .bucketBy(NUM_BUCKETS, "customer_id")
        .sortBy("customer_id")
        .saveAsTable("orders_bucketed_b")
    )


def main():
    spark = get_spark()
    spark.sql("DROP TABLE IF EXISTS orders_unbucketed")
    spark.sql("DROP TABLE IF EXISTS orders_bucketed_a")
    spark.sql("DROP TABLE IF EXISTS orders_bucketed_b")

    orders = load_orders(spark)
    write_unbucketed_table(spark, orders)
    write_bucketed_tables(spark, orders)

    print("=== Join plan: unbucketed self-join on customer_id ===")
    unbucketed = spark.table("orders_unbucketed")
    unbucketed.alias("a").join(
        unbucketed.alias("b"), "customer_id"
    ).explain()
    # Expect Exchange (shuffle) operators on both sides in this plan.

    print("\n=== Join plan: bucketed tables joined on customer_id ===")
    a = spark.table("orders_bucketed_a")
    b = spark.table("orders_bucketed_b")
    a.join(b, "customer_id").explain()
    # Expect no Exchange on the bucketed columns — Spark trusts the
    # existing bucketing and reads directly into the join.


if __name__ == "__main__":
    main()
