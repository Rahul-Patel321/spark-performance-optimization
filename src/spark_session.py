from pyspark.sql import SparkSession


def get_spark(app_name="spark-perf-optimization", extra_conf=None):
    builder = SparkSession.builder.appName(app_name)
    if extra_conf:
        for k, v in extra_conf.items():
            builder = builder.config(k, v)
    return builder.getOrCreate()
