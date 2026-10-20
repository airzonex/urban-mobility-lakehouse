from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.appName("hello-spark").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

try:
    print(f"Spark version: {spark.version}")
    print(f"Spark master: {spark.sparkContext.master}")

    numbers = spark.range(0, 10, numPartitions=2)

    print(f"Partitions: {numbers.rdd.getNumPartitions()}")

    result = (
        numbers.withColumn("group_id", F.col("id") % 2)
        .groupBy("group_id")
        .agg(F.count("*").alias("row_count"), F.sum("id").alias("total"))
        .orderBy("group_id")
    )

    result.show()

    actual = [(row.group_id, row.row_count, row.total) for row in result.collect()]

    expected = [(0, 5, 20), (1, 5, 25)]

    if actual != expected:
        raise RuntimeError(f"Unexpected result: {actual}")

    print("HELLO SPARK PASSED")
finally:
    spark.stop()
