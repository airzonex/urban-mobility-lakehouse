from datetime import date
from decimal import Decimal

from load_silver_trips import (
    create_silver_table,
    write_silver_month,
)
from pyspark.sql import SparkSession


def test_write_silver_month_does_not_duplicate_rows(
    spark: SparkSession,
    silver_table: str,
) -> None:
    source_month = date(2024, 1, 1)

    expected = [
        (1, Decimal("12.50"), source_month),
        (2, Decimal("20.00"), source_month),
    ]
    
    silver = spark.createDataFrame(
        expected,
        schema=(
            "trip_id BIGINT, "
            "total_amount DECIMAL(18, 2), "
            "source_month DATE"
        ),
    )

    create_silver_table(
        spark,
        silver,
        table_name=silver_table,
    )

    for _ in range(2):
        write_silver_month(
            silver,
            source_month=source_month,
            table_name=silver_table,
        )

    actual = [
        (row.trip_id, row.total_amount, row.source_month)
        for row in (
            spark.table(silver_table)
            .orderBy("trip_id")
            .collect()
        )
    ]

    assert actual == expected