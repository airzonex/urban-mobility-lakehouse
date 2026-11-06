import argparse

from common import spark_session
from pyspark.sql import functions as F


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

        # агрегаты
        trips.select(
            F.min("tpep_pickup_datetime").alias("min_pickup"),
            F.max("tpep_pickup_datetime").alias("max_pickup"),
            F.min("tpep_dropoff_datetime").alias("min_dropoff"),
            F.max("tpep_dropoff_datetime").alias("max_dropoff"),
        ).show(truncate=False)

        trips.select(
            *[
                F.count(F.when(F.col(column).isNull(), 1)).alias(column)
                for column in trips.columns
            ]
        ).show(truncate=False)

        trips.select(
            F.count(
                F.when(F.col("passenger_count") == 0, 1)
            ).alias("zero_passenger_count"),
            F.count(
                F.when(F.col("trip_distance") <= 0, 1)
            ).alias("nonpositive_distance"),
            F.count(
                F.when(
                    F.col("tpep_dropoff_datetime")
                    <= F.col("tpep_pickup_datetime"),
                    1,
                )
            ).alias("nonpositive_duration"),
            F.count(
                F.when(F.col("total_amount") < 0, 1)
            ).alias("negative_total_amount"),
        ).show(truncate=False)

        trips.groupBy(
            F.date_format("tpep_pickup_datetime", "yyyy-MM").alias("pickup_month")
        ).count().orderBy("pickup_month").show(100, truncate=False)

        nullable_columns = [
            "passenger_count",
            "RatecodeID",
            "store_and_fwd_flag",
            "congestion_surcharge",
            "Airport_fee",
        ]

        trips.groupBy(
            *[
                F.col(column).isNull().alias(f"{column}_is_null")
                for column in nullable_columns
            ]
        ).count().orderBy(F.desc("count")).show(truncate=False)

        trips.filter(
            F.col("total_amount") < 0
        ).select(
            "VendorID",
            "tpep_pickup_datetime",
            "payment_type",
            "trip_distance",
            "fare_amount",
            "tip_amount",
            "total_amount",
        ).show(20, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()