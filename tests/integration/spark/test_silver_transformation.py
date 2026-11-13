from datetime import date, datetime
from decimal import Decimal

import pytest
from load_silver_trips import (
    transform_trips,
    validate_source_schema,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


@pytest.mark.parametrize(
    ("raw_amount", "expected"),
    [
        pytest.param(12.345, Decimal("12.35"), id="positive-rounding"),
        pytest.param(-12.345, Decimal("-12.35"), id="negative-rounding"),
        pytest.param(0.0, Decimal("0.0"), id="zero"),
        pytest.param(None, None, id="null"),
    ]
)
def test_transform_trips_converts_total_amount_to_decimal(
    spark: SparkSession,
    raw_trip: dict[str, object],
    raw_schema: str,
    raw_amount: float | None,
    expected: Decimal | None,
) -> None:
    raw_trip["total_amount"] = raw_amount
    trips = spark.createDataFrame([raw_trip], schema=raw_schema)

    silver = transform_trips(
        trips,
        source_month=date(2024, 1, 1),
        source_file="s3a://raw/test/yellow_tripdata_2024-01.parquet",
        source_sha256="a" * 64,
    )

    assert silver.schema["total_amount"].dataType == DecimalType(18, 2)

    actual = silver.select("total_amount").collect()

    assert len(actual) == 1
    assert actual[0].total_amount == expected


@pytest.mark.parametrize(
    ("dropoff", "expected_seconds", "expected_flag"),
    [
        pytest.param(
            datetime(2024, 1, 15, 10, 10),
            600,
            False,
            id="positive",
        ),
        pytest.param(
            datetime(2024, 1, 15, 10, 0),
            0,
            True,
            id="zero",
        ),
        pytest.param(
            datetime(2024, 1, 15, 9, 59),
            -60,
            True,
            id="negative",
        ),
        pytest.param(
            datetime(2024, 1, 15, 10, 0, 0, 500000),
            0,
            False,
            id="positive-subseconds",
        ),
        pytest.param(
            None,
            None,
            None,
            id="missing-dropoff",
        ),
    ]
)
def transform_trips_trips_calculates_duration_and_flag(
    spark: SparkSession,
    raw_trip: dict[str, object],
    raw_schema: str,
    dropoff: datetime | None,
    expected_seconds: int | None,
    expected_flag: bool | None,
) -> None:
    raw_trip["tpep_pickup_datetime"] = datetime(2024, 1, 15, 10, 0)
    raw_trip["tpep_dropoff_datetime"] = dropoff

    trips = spark.createDataFrame([raw_trip], schema=raw_schema)

    silver = transform_trips(
        trips,
        source_month=date(2024, 1, 1),
        source_file="s3a://raw/test/yellow_tripdata_2024-01.parquet",
        source_sha256="a" * 64,
    )

    actual = silver.select(
        "duration_seconds",
        "has_nonpositive_duration",
    ).collect()

    assert len(actual) == 1
    assert actual[0].duration_seconds == expected_seconds
    assert actual[0].has_nonpositive_duration is expected_flag


@pytest.mark.parametrize(
    ("pickup", "expected_date", "expected_flag"),
    [
        pytest.param(
            datetime(2023, 12, 31, 23, 59, 59),
            date(2023, 12, 31),
            True,
            id="before-source-month",
        ),
        pytest.param(
            datetime(2024, 1, 1, 0, 0),
            date(2024, 1, 1),
            False,
            id="start-of-source-month",
        ),
        pytest.param(
            datetime(2024, 1, 31, 23, 59, 59),
            date(2024, 1, 31),
            False,
            id="end-of-source-month",
        ),
        pytest.param(
            datetime(2024, 2, 1, 0, 0),
            date(2024, 2, 1),
            True,
            id="after-source-month",
        ),
        pytest.param(
            datetime(2023, 1, 15, 10, 0),
            date(2023, 1, 15),
            True,
            id="same-month-different-year",
        ),
        pytest.param(
            None,
            None,
            None,
            id="missing-pickup",
        ),
    ]
)
def test_transform_trips_flags_pickups_outside_source_month(
    spark: SparkSession,
    raw_trip: dict[str, object],
    raw_schema: str,
    pickup: datetime | None,
    expected_date: date | None,
    expected_flag: bool | None,
) -> None:
    raw_trip["tpep_pickup_datetime"] = pickup
    raw_trip["tpep_dropoff_datetime"] = pickup

    trips = spark.createDataFrame([raw_trip], schema=raw_schema)

    silver = transform_trips(
        trips,
        source_month=date(2024, 1, 1),
        source_file="s3a://raw/test/yellow_tripdata_2024-01.parquet",
        source_sha256="a" * 64,
    )

    actual = silver.select(
        "pickup_date",
        "source_month",
        "is_outside_source_month",
    ).collect()

    assert len(actual) == 1
    assert actual[0].pickup_date == expected_date
    assert actual[0].source_month == date(2024, 1, 1)
    assert actual[0].is_outside_source_month is expected_flag


@pytest.mark.parametrize(
    ("passenger_count", "expected_missing", "expected_zero"),
    [
        pytest.param(1, False, False, id="positive"),
        pytest.param(0, False, True, id="zero"),
        pytest.param(None, True, None, id="missing"),
    ]
)
def test_transform_trips_flags_passenger_count(
    spark: SparkSession,
    raw_trip: dict[str, object],
    raw_schema: str,
    passenger_count: int | None,
    expected_missing: bool | None,
    expected_zero: bool | None,
) -> None:
    raw_trip["passenger_count"] = passenger_count

    trips = spark.createDataFrame([raw_trip], schema=raw_schema)

    silver = transform_trips(
        trips,
        source_month=date(2024, 1, 1),
        source_file="s3a://raw/test/yellow_tripdata_2024-01.parquet",
        source_sha256="a" * 64,
    )

    actual = silver.select(
        "passenger_count",
        "has_missing_passenger_count",
        "has_zero_passenger_count",
    ).collect()

    assert len(actual) == 1
    assert actual[0].passenger_count == passenger_count
    assert actual[0].has_missing_passenger_count is expected_missing
    assert actual[0].has_zero_passenger_count is expected_zero


@pytest.mark.parametrize(
    ("distance", "expected_flag"),
    [
        pytest.param(2.5, False, id="positive"),
        pytest.param(0.0, True, id="zero"),
        pytest.param(-1.0, True, id="negative"),
        pytest.param(None, None, id="missing"),
    ]
)
def test_transform_trips_flags_nonpositive_distance(
    spark: SparkSession,
    raw_trip: dict[str, object],
    raw_schema: str,
    distance: float | None,
    expected_flag: bool | None, 
) -> None:
    raw_trip["trip_distance"] = distance

    trips = spark.createDataFrame([raw_trip], schema=raw_schema)

    silver = transform_trips(
        trips,
        source_month=date(2024, 1, 1),
        source_file="s3a://raw/test/yellow_tripdata_2024-01.parquet",
        source_sha256="a" * 64,
    )

    actual = silver.select(
        "trip_distance",
        "has_nonpositive_distance",
    ).collect()

    assert len(actual) == 1
    assert actual[0].trip_distance == distance
    assert actual[0].has_nonpositive_distance is expected_flag


@pytest.mark.parametrize(
    ("raw_amount", "expected_amount", "expected_flag"),
    [
        pytest.param(12.50, Decimal("12.50"), False, id="positive"),
        pytest.param(0.0, Decimal("0.00"), False, id="zero"),
        pytest.param(-12.50, Decimal("-12.50"), True, id="negative"),
        pytest.param(-0.004, Decimal("0.00"), False, id="rounds-to-zero"),
        pytest.param(-0.006, Decimal("-0.01"), True, id="rounds-to-negative-cent"),
        pytest.param(None, None, None, id="missing"),
    ]
)
def test_transform_trips_flags_negative_total_amount(
    spark: SparkSession,
    raw_trip: dict[str, object],
    raw_amount: float | None,
    raw_schema: str,
    expected_amount: Decimal | None,
    expected_flag: bool | None,
) -> None:
    raw_trip["total_amount"] = raw_amount

    trips = spark.createDataFrame([raw_trip], schema=raw_schema)

    silver = transform_trips(
        trips,
        source_month=date(2024, 1, 1),
        source_file="s3a://raw/test/yellow_tripdata_2024-01.parquet",
        source_sha256="a" * 64,
    )

    actual = silver.select(
        "total_amount",
        "has_negative_total_amount",
    ).collect()

    assert len(actual) == 1
    assert actual[0].total_amount == expected_amount
    assert actual[0].has_negative_total_amount is expected_flag


def test_transform_trips_preserves_duplicates_and_provenance(
    spark: SparkSession,
    raw_trip: dict[str, object],
    raw_schema: str,
) -> None:
    trips = spark.createDataFrame(
        [raw_trip.copy(), raw_trip.copy()],
        schema=raw_schema,
    )

    source_month = date(2024, 1, 1)
    source_sha256 = "b" * 64
    source_file = (
        "s3a://raw/tlc/yellow/year=2024/month=01/"
        f"sha256={source_sha256}/"
        "yellow_tripdata_2024-01.parquet"
    )

    silver = transform_trips(
        trips,
        source_month=source_month,
        source_file=source_file,
        source_sha256=source_sha256,
    )

    actual = silver.collect()

    assert len(actual) == 2
    assert actual[0] == actual[1]

    for row in actual:
        assert row.source_month == source_month
        assert row.source_file == source_file
        assert row.source_sha256 == source_sha256


def test_validate_source_schema_rejects_missing_column(
    spark: SparkSession,
    raw_schema: str,
) -> None:
    trips = spark.createDataFrame([], schema=raw_schema)
    trips = trips.drop("total_amount")

    with pytest.raises(
        ValueError,
        match="Unexpected source schema",
    ) as exc_info:
        validate_source_schema(trips)

    message = str(exc_info.value)

    assert "missing=['total_amount']" in message
    assert "unexpected=[]" in message
    assert "type_mismatches={}" in message


def test_validate_source_schema_rejects_wrong_type(
    spark: SparkSession,
    raw_schema: str,
) -> None:
    trips = spark.createDataFrame([], schema=raw_schema)
    trips = trips.withColumn(
        "total_amount",
        F.col("total_amount").cast("string"),
    )

    with pytest.raises(
        ValueError,
        match="Unexpected source schema",
    ) as exc_info:
        validate_source_schema(trips)

    message = str(exc_info.value)

    assert "missing=[]" in message
    assert "unexpected=[]" in message
    assert (
        "'total_amount': {'expected': 'double', 'actual': 'string'}"
        in message
    )


def test_validate_source_schema_rejects_unexpected_column(
    spark: SparkSession,
    raw_schema: str,
) -> None:
    trips = spark.createDataFrame([], schema=raw_schema)
    trips = trips.withColumn(
        "extra_field",
        F.lit("unexpected"),
    )

    with pytest.raises(
        ValueError,
        match="Unexpected source schema",
    ) as exc_info:
        validate_source_schema(trips)

    message = str(exc_info.value)

    assert "missing=[]" in message
    assert "unexpected=['extra_field']" in message
    assert "type_mismatches={}" in message


def test_validate_source_schema_accepts_expected_schema(
    spark: SparkSession,
    raw_schema: str,
) -> None:
    trips = spark.createDataFrame([], schema=raw_schema)

    validate_source_schema(trips)