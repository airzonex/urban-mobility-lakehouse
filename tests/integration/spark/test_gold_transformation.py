from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from load_gold_daily_pickup_zone import build_daily_pickup_zone_stats


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


def test_daily_pickup_zone_stats_aggregates_trips(
    spark: SparkSession,
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
                False,
            ),
            (
                date(2024, 1, 1),
                date(2024, 1, 15),
                140,
                1.5,
                300,
                Decimal("7.25"),
                False,
                False,
                False,
                False,
            ),
        ],
        schema=SILVER_SCHEMA,
    )

    actual = build_daily_pickup_zone_stats(silver).collect()

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


@pytest.mark.parametrize(
    "flag_name",
    [
        "is_outside_source_month",
        "has_nonpositive_duration",
        "has_nonpositive_distance",
        "has_negative_total_amount",
    ],
)
@pytest.mark.parametrize(
    "flag_value",
    [True, None],
    ids=["true", "null"],
)
def test_daily_pickup_zone_stats_excludes_ineligible_trips(
    spark: SparkSession,
    flag_name: str,
    flag_value: bool | None,
) -> None:
    eligible = {
        "source_month": date(2024, 1, 1),
        "pickup_date": date(2024, 1, 15),
        "pickup_location_id": 140,
        "trip_distance": 2.5,
        "duration_seconds": 600,
        "total_amount": Decimal("12.50"),
        "is_outside_source_month": False,
        "has_nonpositive_duration": False,
        "has_nonpositive_distance": False,
        "has_negative_total_amount": False,
    }

    ineligible = {
        **eligible,
        flag_name: flag_value,
        "trip_distance": 10.0,
        "duration_seconds": 3600,
        "total_amount": Decimal("100.00"),
    }

    silver = spark.createDataFrame(
        [eligible, ineligible],
        schema=SILVER_SCHEMA,
    )

    actual = build_daily_pickup_zone_stats(silver).collect()

    assert len(actual) == 1
    assert actual[0].asDict() == {
        "source_month": date(2024, 1, 1),
        "pickup_date": date(2024, 1, 15),
        "pickup_location_id": 140,
        "trip_count": 1,
        "total_distance_miles": 2.5,
        "total_duration_seconds": 600,
        "total_amount_sum": Decimal("12.50"),
    }


@pytest.mark.parametrize(
    ("changed_column", "changed_value"),
    [
        ("source_month", date(2024, 2, 1)),
        ("pickup_date", date(2024, 1, 16)),
        ("pickup_location_id", 141),
    ],
    ids=["different-source-month", "different-pickup-date", "different-zone"],
)
def test_daily_pickup_zone_stats_separates_groups(
    spark: SparkSession,
    changed_column: str,
    changed_value: date | int,
) -> None:
    first = {
        "source_month": date(2024, 1, 1),
        "pickup_date": date(2024, 1, 15),
        "pickup_location_id": 140,
        "trip_distance": 2.5,
        "duration_seconds": 600,
        "total_amount": Decimal("12.50"),
        "is_outside_source_month": False,
        "has_nonpositive_duration": False,
        "has_nonpositive_distance": False,
        "has_negative_total_amount": False,
    }

    second = {
        **first,
        changed_column: changed_value,
    }

    silver = spark.createDataFrame(
        [first, second],
        schema=SILVER_SCHEMA,
    )

    actual = build_daily_pickup_zone_stats(silver).collect()

    assert len(actual) == 2

    actual_keys = {
        (
            row.source_month,
            row.pickup_date,
            row.pickup_location_id,
        )
        for row in actual
    }

    expected_keys = {
        (
            record["source_month"],
            record["pickup_date"],
            record["pickup_location_id"],
        )
        for record in [first, second]
    }

    assert actual_keys == expected_keys

    for row in actual:
        assert row.trip_count == 1
        assert row.total_distance_miles == 2.5
        assert row.total_duration_seconds == 600
        assert row.total_amount_sum == Decimal("12.50")


@pytest.mark.parametrize(
    "has_input",
    [False, True],
    ids=["empty-input", "all-trips-excluded"],
)
def test_daily_pickup_zone_stats_returns_empty_when_no_eligible_trips(
    spark: SparkSession,
    has_input: bool,
) -> None:
    rows = []

    if has_input:
        rows.append(
            {
                "source_month": date(2024, 1, 1),
                "pickup_date": date(2024, 1, 15),
                "pickup_location_id": 140,
                "trip_distance": 2.5,
                "duration_seconds": 600,
                "total_amount": Decimal("-12.50"),
                "is_outside_source_month": False,
                "has_nonpositive_duration": False,
                "has_nonpositive_distance": False,
                "has_negative_total_amount": True,
            }
        )

    silver = spark.createDataFrame(
        rows,
        schema=SILVER_SCHEMA,
    )

    actual = build_daily_pickup_zone_stats(silver).collect()

    assert actual == []