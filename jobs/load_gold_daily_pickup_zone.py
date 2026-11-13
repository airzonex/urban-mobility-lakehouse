from datetime import date


from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


GOLD_TABLE = "lakehouse.gold.daily_pickup_zone_stats"


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


def create_gold_table(
    spark: SparkSession,
    gold: DataFrame,
    *,
    table_name: str = GOLD_TABLE,
) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

    if spark.catalog.tableExists(table_name):
        return

    (
        gold.limit(0)
        .writeTo(table_name)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .partitionedBy("source_month")
        .create()
    )


def write_gold_month(
    gold: DataFrame,
    *,
    source_month: date,
    table_name: str = GOLD_TABLE,
) -> None:
    gold.writeTo(table_name).overwrite(
        F.col("source_month") == F.lit(source_month)
    )