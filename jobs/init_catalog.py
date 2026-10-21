from common import spark_session


def main() -> None:
    spark = spark_session("iceberg-catalog-init")

    try:
        for layer in ("silver", "gold"):
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{layer}")

        spark.sql("SHOW NAMESPACES IN lakehouse").show()

        print("CATALOG INIT PASSED")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
