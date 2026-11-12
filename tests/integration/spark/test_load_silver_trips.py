import json
import os
from contextlib import closing
from datetime import date
from decimal import Decimal
from urllib.parse import urlsplit

import boto3
import pytest
from botocore.config import Config
from load_silver_trips import (
    create_silver_table,
    write_silver_month,
    load_silver_trips,
    transform_trips,
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


def test_load_silver_trips_loads_raw_file(
    spark: SparkSession,
    silver_table: str,
    raw_source: str,
) -> None:
    load_silver_trips(
        spark,
        input_path=raw_source,
        year=2024,
        month=1,
        table_name=silver_table,
    )

    actual = spark.table(silver_table).collect()

    assert len(actual) == 1

    row = actual[0]
    assert row.vendor_id == 1
    assert row.total_amount == Decimal("12.50")
    assert row.pickup_date == date(2024, 1, 15)
    assert row.duration_seconds == 600
    assert row.source_file == raw_source

    expected_sha256 = raw_source.split("/sha256=", 1)[1].split("/", 1)[0]
    assert row.source_sha256 == expected_sha256

    assert row.has_negative_total_amount is False
    assert row.has_nonpositive_duration is False


def test_load_silver_trips_preserves_data_when_metadata_is_invalid(
    spark: SparkSession,
    silver_table: str,
    raw_source: str,
) -> None:
    # load the original source
    load_silver_trips(
        spark,
        input_path=raw_source,
        year=2024,
        month=1,
        table_name=silver_table,
    )

    before = spark.table(silver_table).collect()
    assert len(before) == 1

    snapshots_before = spark.sql(
        f"SELECT snapshot_id FROM {silver_table}.snapshots"
    ).collect()

    # change metadata's month
    uri = urlsplit(raw_source)
    bucket = uri.netloc
    parquet_key = uri.path.lstrip("/")
    metadata_key = (
        parquet_key.rsplit("/", 1)[0] + "/metadata.json"
    )

    with closing(
        boto3.client(
            "s3",
            endpoint_url=os.environ["S3_ENDPOINT"],
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            region_name=os.environ["AWS_REGION"],
            config=Config(s3={"addressing_style": "path"}),
        )
    ) as s3:
        response = s3.get_object(
            Bucket=bucket,
            Key=metadata_key,
        )

        with closing(response["Body"]) as body:
            metadata = json.loads(body.read())

        metadata["month"] = 2

        s3.put_object(
            Bucket=bucket,
            Key=metadata_key,
            Body=json.dumps(metadata).encode("utf-8"),
            ContentType="application/json",
        )

    # the january load must reject february metadata
    with pytest.raises(
        ValueError,
        match="Source metadata mismatch for month",
    ):
        load_silver_trips(
            spark,
            input_path=raw_source,
            year=2024,
            month=1,
            table_name=silver_table,
        )

    # assert neither the rows nor the snapshots changed
    after = spark.table(silver_table).collect()
    snapshots_after = spark.sql(
        f"SELECT snapshot_id FROM {silver_table}.snapshots"
    ).collect()

    assert after == before
    assert {row.snapshot_id for row in snapshots_after} == {
        row.snapshot_id for row in snapshots_before
    }


@pytest.mark.parametrize("raw_source_rows", [[]], ids=["empty-source"])
def test_load_silver_trips_preserves_data_when_source_is_empty(
    spark: SparkSession,
    silver_table: str,
    raw_source: str,
    raw_trip: dict[str, object],
    raw_schema: str,
) -> None:
    trips = spark.createDataFrame([raw_trip], schema=raw_schema)

    initial = transform_trips(
        trips,
        source_month=date(2024, 1, 1),
        source_file="s3a://test-existing/previous.parquet",
        source_sha256="a" * 64,
    )

    create_silver_table(
        spark,
        initial,
        table_name=silver_table,
    )
    initial.writeTo(silver_table).append()

    before = spark.table(silver_table).collect()
    assert len(before) == 1

    snapshots_before = {
        row.snapshot_id
        for row in spark.sql(
            f"SELECT snapshot_id FROM {silver_table}.snapshots"
        ).collect()
    }

    # attempt to replace january with an empty source
    with pytest.raises(
        ValueError,
        match="Source Parquet contains no rows",
    ):
        load_silver_trips(
            spark,
            input_path=raw_source,
            year=2024,
            month=1,
            table_name=silver_table,
        )

    # assert no data change and no new commit
    after = spark.table(silver_table).collect()

    snapshots_after = {
        row.snapshot_id
        for row in spark.sql(
            f"SELECT snapshot_id FROM {silver_table}.snapshots"
        ).collect()
    }

    assert after == before
    assert snapshots_after == snapshots_before