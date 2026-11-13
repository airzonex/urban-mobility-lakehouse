from datetime import date
from decimal import Decimal

import pytest
from load_gold_daily_pickup_zone import (
    create_gold_table,
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