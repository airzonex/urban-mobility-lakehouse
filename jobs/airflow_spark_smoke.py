from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("airflow-spark-smoke")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    try:
        result = (
            spark.range(10)
            .agg(
                F.count("*").alias("row_count"),
                F.sum("id").alias("total"),
            )
            .first()
        )

        if result is None:
            raise RuntimeError("Spark returned no result")

        if result["row_count"] != 10 or result["total"] != 45:
            raise RuntimeError(f"Unexpected Spark result: {result}")

        print("AIRFLOW_SPARK_SMOKE_PASSED: rows=10 total=45")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()