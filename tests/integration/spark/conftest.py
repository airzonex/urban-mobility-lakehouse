from collections.abc import Iterator
from datetime import datetime
from uuid import uuid4

import pytest
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
