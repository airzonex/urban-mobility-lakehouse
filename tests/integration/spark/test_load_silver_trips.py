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

def test_write_silver_month_replaces_only_requested_month(
    spark: SparkSession,
    silver_table: str,
) -> None:
    january = date(2024, 1, 1)
    february = date(2024, 2, 1)

    schema = (
        "trip_id BIGINT, "
        "total_amount DECIMAL(18, 2), "
        "source_month DATE"
    )

    initial_rows = (
        (1, Decimal("12.50"), january),
        (2, Decimal("20.00"), january),
        (3, Decimal("30.00"), february),
    )

    initial = spark.createDataFrame(
        data=initial_rows,
        schema=schema,
    )

    create_silver_table(
        spark,
        initial,
        table_name=silver_table,
    )

    initial.writeTo(silver_table).append()

    replacement = spark.createDataFrame(
        [
            (1, Decimal("15.00"), january),
            (4, Decimal("40.00"), january),
        ],
        schema=schema,
    )

    write_silver_month(
        replacement,
        source_month=january,
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

    expected = [
        (1, Decimal("15.00"), january),
        (3, Decimal("30.00"), february),
        (4, Decimal("40.00"), january),
    ]
    
    assert actual == expected

def test_write_silver_month_empty_input_clears_only_requested_month(
    spark: SparkSession,
    silver_table: str,
) -> None:
    january = date(2024, 1, 1)
    february = date(2024, 2, 1)

    schema = (
        "trip_id BIGINT, "
        "total_amount DECIMAL(18, 2), "
        "source_month DATE"
    )

    initial = spark.createDataFrame(
        [
            (1, Decimal("12.50"), january),
            (2, Decimal("20.00"), january),
            (3, Decimal("30.00"), february),
        ],
        schema=schema,
    )

    create_silver_table(
        spark,
        initial,
        table_name=silver_table,
    )
    
    initial.writeTo(silver_table).append()

    empty = spark.createDataFrame([], schema=schema)

    write_silver_month(
        empty,
        source_month=january,
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

    expected = [
        (3, Decimal("30.00"), february),
    ]

    assert actual == expected

