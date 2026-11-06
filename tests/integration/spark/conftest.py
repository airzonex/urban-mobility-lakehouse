from collections.abc import Iterator
from uuid import uuid4

import pytest
from common import spark_session
from pyspark.sql import SparkSession


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