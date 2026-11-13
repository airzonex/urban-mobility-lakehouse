import hashlib
import json
import os
from collections.abc import Iterator
from contextlib import closing
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import boto3
import pytest
from botocore.config import Config
from common import spark_session
from pyspark.sql import SparkSession

RAW_SCHEMA = """
    VendorID INT,
    tpep_pickup_datetime TIMESTAMP_NTZ,
    tpep_dropoff_datetime TIMESTAMP_NTZ,
    passenger_count BIGINT,
    trip_distance DOUBLE,
    RatecodeID BIGINT,
    store_and_fwd_flag STRING,
    PULocationID INT,
    DOLocationID INT,
    payment_type BIGINT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    Airport_fee DOUBLE
"""


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = spark_session("silver-integration-tests")

    try:
        yield session
    finally:
        session.stop()


@pytest.fixture
def silver_table(spark: SparkSession) -> Iterator[str]:
    table_name = (
        f"lakehouse.silver.test_yellow_trips_{uuid4().hex}"
    )

    try:
        yield table_name
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name} PURGE")


@pytest.fixture
def raw_schema() -> str:
    return RAW_SCHEMA


@pytest.fixture
def raw_trip() -> dict[str, object]:
    return {
        "VendorID": 1,
        "tpep_pickup_datetime": datetime(2024, 1, 15, 10, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 15, 10, 10),
        "passenger_count": 1,
        "trip_distance": 2.0,
        "RatecodeID": 1,
        "store_and_fwd_flag": "N",
        "PULocationID": 140,
        "DOLocationID": 236,
        "payment_type": 1,
        "fare_amount": 10.0,
        "extra": 0.0,
        "mta_tax": 0.0,
        "tip_amount": 0.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 0.0,
        "total_amount": 12.50,
        "congestion_surcharge": 0.0,
        "Airport_fee": 0.0,
    }


@pytest.fixture
def raw_source_rows(
    raw_trip: dict[str, object],
) -> list[dict[str, object]]:
    return [raw_trip]


@pytest.fixture
def raw_source(
    spark: SparkSession,
    raw_source_rows: list[dict[str, object]],
    raw_schema: str,
    tmp_path: Path,
) -> Iterator[str]:
    output_dir = tmp_path / "parquet"

    trips = spark.createDataFrame(raw_source_rows, schema=raw_schema)
    trips.coalesce(1).write.parquet(output_dir.as_uri())

    parquet_files = list(output_dir.glob("part-*.parquet"))
    assert len(parquet_files) == 1
    parquet_file = parquet_files[0]

    source_sha256 = hashlib.sha256(
        parquet_file.read_bytes()
    ).hexdigest()

    filename = "yellow_tripdata_2024-01.parquet"
    prefix = (
        "tlc/yellow/year=2024/month=01/"
        f"sha256={source_sha256}"
    )
    parquet_key = f"{prefix}/{filename}"
    metadata_key = f"{prefix}/metadata.json"

    metadata = {
        "year": 2024,
        "month": 1,
        "filename": filename,
        "sha256": source_sha256,
        "size_bytes": parquet_file.stat().st_size,
    }

    bucket = f"test-silver-source-{uuid4().hex}"

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
        s3.create_bucket(Bucket=bucket)

        try:
            s3.upload_file(
                Filename=str(parquet_file),
                Bucket=bucket,
                Key=parquet_key
            )

            s3.put_object(
                Bucket=bucket,
                Key=metadata_key,
                Body=json.dumps(metadata).encode("utf-8"),
                ContentType="application/json",
            )

            yield f"s3a://{bucket}/{parquet_key}"

        finally:
            s3.delete_object(Bucket=bucket, Key=metadata_key)
            s3.delete_object(Bucket=bucket, Key=parquet_key)
            s3.delete_bucket(Bucket=bucket)


@pytest.fixture
def gold_table(spark: SparkSession):
    table_name = (
        f"lakehouse.gold.test_daily_pickup_zone_stats_{uuid4().hex}"
    )

    try:
        yield table_name
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name} PURGE")