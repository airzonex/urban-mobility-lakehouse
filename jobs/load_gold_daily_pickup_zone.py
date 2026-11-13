import argparse
import logging
from datetime import date

from common import spark_session
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


GOLD_TABLE = "lakehouse.gold.daily_pickup_zone_stats"
SILVER_TABLE = "lakehouse.silver.yellow_trips"


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


def load_gold_month(
    spark: SparkSession,
    *,
    year: int,
    month: int,
    source_table: str = SILVER_TABLE,
    target_table: str = GOLD_TABLE,
) -> None:
    if not 1000 <= year <= 9999:
        raise ValueError("year must be a four-digit year")

    source_month = date(year, month, 1)

    silver_month = spark.table(source_table).filter(
        F.col("source_month") == F.lit(source_month)
    )

    if silver_month.isEmpty():
        raise ValueError(
            f"No Silver records for source month {source_month}"
        )

    gold = build_daily_pickup_zone_stats(silver_month)

    create_gold_table(
        spark,
        gold,
        table_name=target_table,
    )

    write_gold_month(
        gold,
        source_month=source_month,
        table_name=target_table,
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Build daily pickup-zone statistics from Silver",
    )
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    spark = spark_session("load-gold-daily-pickup-zone")

    try:
        logger.info(
            "Loading Gold: year=%s, month=%s, target=%s",
            args.year,
            args.month,
            GOLD_TABLE,
        )

        load_gold_month(
            spark,
            year=args.year,
            month=args.month,
        )

        logger.info(
            "Gold load committed: year=%s, month=%s, target=%s",
            args.year,
            args.month,
            GOLD_TABLE,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()