import argparse
from decimal import Decimal

from common import spark_session


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--read-only",
        action="store_true",
        help="Check the existing table without writing data",
    )
    args = parser.parse_args()

    spark = spark_session("iceberg-smoke")

    try:
        print(f"Spark version: {spark.version}")

        if not args.read_only:
            spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.demo")

            spark.sql("""
                CREATE TABLE IF NOT EXISTS lakehouse.demo.smoke_trips (
                    trip_id BIGINT,
                    zone STRING,
                    amount DECIMAL(10, 2)
                )
                USING iceberg
                TBLPROPERTIES ('format-version' = '2')
            """)

            spark.sql("""
                INSERT OVERWRITE lakehouse.demo.smoke_trips
                VALUES
                    (
                        CAST(1 AS BIGINT),
                        'Manhattan',
                        CAST(12.50 AS DECIMAL(10, 2))
                    ),
                    (
                        CAST(2 AS BIGINT),
                        'Queens',
                        CAST(20.00 AS DECIMAL(10, 2))
                    ),
                    (
                        CAST(3 AS BIGINT),
                        'Brooklyn',
                        CAST(7.50 AS DECIMAL(10, 2))
                    )
            """)

        spark.sql("""
            SELECT *
            FROM lakehouse.demo.smoke_trips
            ORDER BY trip_id
        """).show()

        result = spark.sql("""
            SELECT
                count(*) AS row_count,
                sum(amount) AS total
            FROM lakehouse.demo.smoke_trips
        """).first()

        if result is None or result.row_count != 3 or result.total != Decimal("40.00"):
            raise RuntimeError(f"Unexpected result: {result}")

        spark.sql("""
            SELECT committed_at, snapshot_id, operation
            FROM lakehouse.demo.smoke_trips.snapshots
            ORDER BY committed_at
        """).show(truncate=False)

        print("ICEBERG READ PASSED: rows=3 total=40.00")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
