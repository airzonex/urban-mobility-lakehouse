from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_daily_pickup_zone_stats(silver: DataFrame) -> DataFrame:
    eligible = silver.filter(
        (~F.col("is_outside_source_month"))
        & (~F.col("has_nonpositive_duration"))
        & (~F.col("has_nonpositive_distance"))
        & (~F.col("has_negative_total_amount"))
    )

    return eligible.groupBy(
        "source_month",
        "pickup_date",
        "pickup_location_id"
    ).agg(
        F.count("*").alias("trip_count"),
        F.sum("trip_distance").alias("total_distance_miles"),
        F.sum("duration_seconds").alias("total_duration_seconds"),
        F.sum("total_amount").alias("total_amount_sum"),
    )