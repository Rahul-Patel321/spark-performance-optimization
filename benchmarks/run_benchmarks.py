"""
Runs each naive/optimized pair and prints a timing comparison table.

Note: on small local datasets some of these differences will be modest —
the point of this repo is demonstrating *correct use of the API and
understanding of why each technique helps*, which is what interviewers
actually probe for. The gains become dramatic at real cluster scale with
GB/TB-sized data, which local runs can't fully reproduce.
"""
import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from spark_session import get_spark  # noqa: E402
import importlib  # noqa: E402


def time_it(fn, *args):
    start = time.time()
    result = fn(*args)
    if hasattr(result, "count"):
        result.count()  # force the action so timing reflects real execution
    return time.time() - start


def main():
    spark = get_spark()
    results = []

    # --- Broadcast join ---
    bj = importlib.import_module("01_broadcast_join")
    orders, products = bj.load_data(spark)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    t_naive = time_it(bj.naive_join, orders, products)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
    t_opt = time_it(bj.optimized_broadcast_join, orders, products)
    results.append(("Broadcast Join", t_naive, t_opt))

    # --- Shuffle optimization ---
    so = importlib.import_module("08_shuffle_optimization")
    orders2 = spark.read.option("header", True).option("inferSchema", True).csv("data/raw/orders.csv")
    t_naive = time_it(so.naive_approach, spark, orders2)
    t_opt = time_it(so.optimized_approach, spark, orders2)
    results.append(("Shuffle Optimization", t_naive, t_opt))

    print(f"\n{'Technique':<25}{'Naive (s)':<15}{'Optimized (s)':<15}{'Speedup':<10}")
    print("-" * 65)
    for name, naive_t, opt_t in results:
        speedup = f"{naive_t / opt_t:.2f}x" if opt_t > 0 else "n/a"
        print(f"{name:<25}{naive_t:<15.2f}{opt_t:<15.2f}{speedup:<10}")


if __name__ == "__main__":
    main()
