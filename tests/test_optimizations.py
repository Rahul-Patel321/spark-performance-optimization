"""
Correctness tests: every optimization technique must produce the SAME
output as its naive counterpart. These techniques change performance,
never correctness — this suite is what proves that claim in CI.
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "data"))

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[2]").appName("test-spark-perf").getOrCreate()


def test_broadcast_join_matches_shuffle_join(spark):
    import importlib
    bj = importlib.import_module("01_broadcast_join")

    orders = spark.createDataFrame(
        [("o1", "P1", 2), ("o2", "P2", 5), ("o3", "P1", 1)],
        ["order_id", "product_id", "quantity"],
    )
    products = spark.createDataFrame(
        [("P1", "Widget"), ("P2", "Gadget")], ["product_id", "product_name"]
    )

    naive = bj.naive_join(orders, products).orderBy("order_id").collect()
    optimized = bj.optimized_broadcast_join(orders, products).orderBy("order_id").collect()
    assert naive == optimized


def test_repartition_and_coalesce_preserve_row_count(spark):
    df = spark.range(0, 1000).withColumnRenamed("id", "value")
    original_count = df.count()

    assert df.coalesce(2).count() == original_count
    assert df.repartition(6).count() == original_count


def test_shuffle_optimization_matches_naive_totals(spark):
    import importlib
    so = importlib.import_module("08_shuffle_optimization")

    orders = spark.createDataFrame(
        [("North", "C1", 3), ("North", "C2", 4), ("South", "C3", 10)],
        ["region", "customer_id", "quantity"],
    )

    naive = {r["region"]: r["region_total"] for r in so.naive_approach(spark, orders).collect()}
    optimized = {r["region"]: r["region_total"] for r in so.optimized_approach(spark, orders).collect()}
    assert naive == optimized


def test_skew_salting_preserves_row_count(spark):
    import importlib
    sh = importlib.import_module("07_skew_handling")

    orders = spark.createDataFrame(
        [("HOT", "P1", "North", 1), ("HOT", "P2", "South", 2), ("OTHER", "P1", "East", 3)],
        ["customer_id", "product_id", "region", "quantity"],
    )
    customers = orders.select("customer_id").distinct().withColumn("signup_region", F.lit("Unknown"))

    naive_count = sh.naive_skewed_join(orders, customers).count()
    salted_count = sh.salted_join(orders, customers).count()
    assert naive_count == salted_count
