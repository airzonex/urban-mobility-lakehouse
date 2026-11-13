from datetime import date
from decimal import Decimal

import pytest
from load_gold_daily_pickup_zone import (
    create_gold_table,
    load_gold_month,
    write_gold_month,
)
from pyspark.sql import SparkSession

GOLD_SCHEMA = """
    source_month DATE,
    pickup_date DATE,
    pickup_location_id INT,
    trip_count BIGINT,
    total_distance_miles DOUBLE,
    total_duration_seconds BIGINT,
    total_amount_sum DECIMAL(28, 2)
"""

SILVER_SCHEMA = """
    source_month DATE,
    pickup_date DATE,
    pickup_location_id INT,
    trip_distance DOUBLE,
    duration_seconds BIGINT,
    total_amount DECIMAL(18, 2),
    is_outside_source_month BOOLEAN,
    has_nonpositive_duration BOOLEAN,
    has_nonpositive_distance BOOLEAN,
    has_negative_total_amount BOOLEAN
"""


@pytest.mark.parametrize(
    "empty_replacement",
    [False, True],
    ids=["replace-january", "clear-january"],
)
def test_write_gold_month_preserves_other_months(
    spark: SparkSession,
    gold_table: str,
    empty_replacement: bool,
) -> None:
    january = (
        date(2024, 1, 1),
        date(2024, 1, 15),
        140,
        2,
        4.0,
        900,
        Decimal("19.75"),
    )
    february = (
        date(2024, 2, 1),
        date(2024, 2, 10),
        141,
        1,
        3.0,
        600,
        Decimal("15.00"),
    )

    initial = spark.createDataFrame(
        [january, february],
        schema=GOLD_SCHEMA,
    )

    create_gold_table(spark, initial, table_name=gold_table)
    initial.writeTo(gold_table).append()

    updated_january = (
        date(2024, 1, 1),
        date(2024, 1, 20),
        142,
        3,
        9.0,
        1800,
        Decimal("45.00"),
    )

    replacement_rows = (
        [] if empty_replacement else [updated_january]
    )
    replacement = spark.createDataFrame(
        replacement_rows,
        schema=GOLD_SCHEMA,
    )

    write_gold_month(
        replacement,
        source_month=date(2024, 1, 1),
        table_name=gold_table,
    )

    actual = [
        tuple(row)
        for row in spark.table(gold_table)
        .orderBy("source_month", "pickup_date", "pickup_location_id")
        .collect()
    ]

    expected = (
        [february]
        if empty_replacement
        else [updated_january, february]
    )

    assert actual == expected


@pytest.mark.parametrize(
    "load_count",
    [1, 2],
    ids=["first-load", "repeated-load"]
)
def test_load_gold_month_loads_requested_month(
    spark: SparkSession,
    silver_table: str,
    gold_table: str,
    load_count: int,
) -> None:
    silver = spark.createDataFrame(
        [
            # two eligible january trips in the same group
            (
                date(2024, 1, 1), date(2024, 1, 15), 140,
                2.5, 600, Decimal("12.50"),
                False, False, False, False,
            ),
            (
                date(2024, 1, 1), date(2024, 1, 15), 140,
                1.5, 300, Decimal("7.25"),
                False, False, False, False,
            ),
            # january trips excluded by its quality flag
            (
                date(2024, 1, 1), date(2024, 1, 15), 140,
                3.0, 900, Decimal("-10.00"),
                False, False, False, True,
            ),
            # eligible february trip: outside the requested month
            (
                date(2024, 2, 1), date(2024, 2, 10), 141,
                5.0, 1200, Decimal("30.00"),
                False, False, False, False,
            ),
        ],
        schema=SILVER_SCHEMA,
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

    (
        silver.writeTo(silver_table)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .partitionedBy("source_month")
        .create()
    )

    for _ in range(load_count):
        load_gold_month(
            spark,
            year=2024,
            month=1,
            source_table=silver_table,
            target_table=gold_table,
        )

    actual = spark.table(gold_table).collect()

    assert len(actual) == 1
    assert actual[0].asDict() == {
        "source_month": date(2024, 1, 1),
        "pickup_date": date(2024, 1, 15),
        "pickup_location_id": 140,
        "trip_count": 2,
        "total_distance_miles": 4.0,
        "total_duration_seconds": 900,
        "total_amount_sum": Decimal("19.75"),
    }


def test_load_gold_month_preserves_gold_when_source_month_is_missing(
    spark: SparkSession,
    silver_table: str,
    gold_table: str,
) -> None:
    # silver exists, but contains only february
    silver = spark.createDataFrame(
        [
            (
                date(2024, 2, 1),
                date(2024, 2, 10),
                141,
                5.0,
                1200,
                Decimal("30.00"),
                False,
                False,
                False,
                False,
            ),
        ],
        schema=SILVER_SCHEMA,
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

    (
        silver.writeTo(silver_table)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .partitionedBy("source_month")
        .create()
    )

    # gold already contains january results
    existing_gold = spark.createDataFrame(
        [
            (
                date(2024, 1, 1),
                date(2024, 1, 15),
                140,
                2,
                4.0,
                900,
                Decimal("19.75"),
            ),
        ],
        schema=GOLD_SCHEMA,
    )

    create_gold_table(
        spark,
        existing_gold,
        table_name=gold_table,
    )
    existing_gold.writeTo(gold_table).append()

    rows_before = spark.table(gold_table).collect()
    snapshots_before = {
        row.snapshot_id
        for row in spark.table(f"{gold_table}.snapshots").collect()
    }

    with pytest.raises(
        ValueError,
        match="No Silver records for source month 2024-01-01",
    ):
        load_gold_month(
            spark,
            year=2024,
            month=1,
            source_table=silver_table,
            target_table=gold_table,
        )

    rows_after = spark.table(gold_table).collect()
    snapshots_after = {
        row.snapshot_id
        for row in spark.table(f"{gold_table}.snapshots").collect()
    }

    assert rows_before == rows_after
    assert snapshots_before == snapshots_after


def test_load_gold_month_clears_month_when_all_trips_are_excluded(
    spark: SparkSession,
    silver_table: str,
    gold_table: str,
) -> None:
    silver = spark.createDataFrame(
        [
            (
                date(2024, 1, 1),
                date(2024, 1, 15),
                140,
                2.5,
                600,
                Decimal("12.50"),
                False,
                False,
                False,
                True,   # has_negative_total_amount
            ),
        ],
        schema=SILVER_SCHEMA
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

    (
        silver.writeTo(silver_table)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .partitionedBy("source_month")
        .create()
    )

    january = (
        date(2024, 1, 1),
        date(2024, 1, 15),
        140,
        2,
        4.0,
        900,
        Decimal("19.75"),
    )
    february = (
        date(2024, 2, 1),
        date(2024, 2, 10),
        141,
        1,
        3.0,
        600,
        Decimal("15.00"),
    )

    existing_gold = spark.createDataFrame(
        [january, february],
        schema=GOLD_SCHEMA,
    )

    create_gold_table(
        spark,
        existing_gold,
        table_name=gold_table,
    )
    existing_gold.writeTo(gold_table).append()

    load_gold_month(
        spark,
        year=2024,
        month=1,
        source_table=silver_table,
        target_table=gold_table,
    )

    actual = [
        tuple(row)
        for row in spark.table(gold_table).collect()
    ]

    assert actual == [february]