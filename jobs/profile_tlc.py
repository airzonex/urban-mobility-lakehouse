import argparse

from common import spark_session


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True)
    args = parser.parse_args()

    spark = spark_session("profile-tlc")

    try:
        trips = spark.read.parquet(args.input_path)

        trips.printSchema()
        trips.show(10, truncate=False)

        print(f"Row count: {trips.count()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()