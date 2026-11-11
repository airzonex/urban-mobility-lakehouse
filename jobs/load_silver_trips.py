import argparse
import json
import logging
import re
from datetime import date
from urllib.parse import urlsplit

from common import spark_session
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logger = logging.getLogger(__name__)


EXPECTED_RAW_TYPES = {
    "VendorID": T.IntegerType(),
    "tpep_pickup_datetime": T.TimestampNTZType(),
    "tpep_dropoff_datetime": T.TimestampNTZType(),
    "passenger_count": T.LongType(),
    "trip_distance": T.DoubleType(),
    "RatecodeID": T.LongType(),
    "store_and_fwd_flag": T.StringType(),
    "PULocationID": T.IntegerType(),
    "DOLocationID": T.IntegerType(),
    "payment_type": T.LongType(),
    "fare_amount": T.DoubleType(),
    "extra": T.DoubleType(),
    "mta_tax": T.DoubleType(),
    "tip_amount": T.DoubleType(),
    "tolls_amount": T.DoubleType(),
    "improvement_surcharge": T.DoubleType(),
    "total_amount": T.DoubleType(),
    "congestion_surcharge": T.DoubleType(),
    "Airport_fee": T.DoubleType(),
}


MONEY_COLUMNS = [
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
]


SOURCE_TO_SILVER = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "Airport_fee": "airport_fee",
}


SILVER_TABLE = "lakehouse.silver.yellow_trips"


def validate_source_path(
    input_path: str,
    year: int,
    month: int,
) -> str:
    uri = urlsplit(input_path)

    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"

    logger.debug("filename=%s", filename)

    pattern = (
        rf"/tlc/yellow/year={year}/month={month:02d}/"
        rf"sha256=([0-9a-f]{{64}})/{re.escape(filename)}"
    )

    logger.debug("pattern=%s", pattern)
    logger.debug("uri.path=%s", uri.path)

    match = re.fullmatch(pattern, uri.path)

    if (
        uri.scheme != "s3a"
        or not uri.netloc
        or uri.query
        or uri.fragment
        or match is None
    ):
        raise ValueError(
            "Expected an exact s3a:// raw file URI "
            f"for {year}-{month:02d}"
        )

    return match.group(1)


def read_source_metadata(
    spark: SparkSession,
    input_path: str,
) -> dict:
    jvm = spark._jvm
    hadoop_config = spark.sparkContext._jsc.hadoopConfiguration()

    parquet_path = jvm.org.apache.hadoop.fs.Path(input_path)
    metadata_path = jvm.org.apache.hadoop.fs.Path(
        parquet_path.getParent(),
        "metadata.json",
    )

    filesystem = metadata_path.getFileSystem(hadoop_config)

    status = filesystem.getFileStatus(metadata_path)
    if not status.isFile() or status.getLen() > 1024 * 1024:
        raise ValueError(
            "metadata.json must be a file no larger than 1 MiB"
        )

    stream = filesystem.open(metadata_path)
    try:
        content = jvm.org.apache.commons.io.IOUtils.toString(
            stream,
            "UTF-8",
        )
    finally:
        stream.close()

    metadata = json.loads(content)

    if not isinstance(metadata, dict):
        raise ValueError("metadata.json must contain a JSON object")

    return metadata


def validate_source_metadata(
    spark: SparkSession,
    metadata: dict,
    input_path: str,
    year: int,
    month: int,
    source_sha256: str,
) -> None:
    expected = {
        "year": year,
        "month": month,
        "filename": f"yellow_tripdata_{year}-{month:02d}.parquet",
        "sha256": source_sha256,
    }

    for field, expected_value in expected.items():
        actual_value = metadata.get(field)

        if (
            type(actual_value) is not type(expected_value)
            or actual_value != expected_value
        ):
            raise ValueError(
                f"Source metadata mismatch for {field}: "
                f"expected={expected_value!r}, actual={actual_value!r}"
            )

    size_bytes = metadata.get("size_bytes")

    if type(size_bytes) is not int or size_bytes <= 0:
        raise ValueError(f"size_bytes must be a positive integer: {size_bytes!r}")

    jvm = spark._jvm
    hadoop_config = spark.sparkContext._jsc.hadoopConfiguration()

    parquet_path = jvm.org.apache.hadoop.fs.Path(input_path)
    filesystem = parquet_path.getFileSystem(hadoop_config)
    status = filesystem.getFileStatus(parquet_path)

    if not status.isFile():
        raise ValueError(f"Source path is not a file: {input_path}")

    actual_size = status.getLen()

    if actual_size != size_bytes:
        raise ValueError(
            "Source Parquet size mismatch: "
            f"metadata={size_bytes}, actual={actual_size}"
        )


def validate_source_schema(trips: DataFrame) -> None:
    actual_types = {
        field.name: field.dataType
        for field in trips.schema.fields
    }

    missing_columns = sorted(
        EXPECTED_RAW_TYPES.keys() - actual_types.keys()
    )

    unexpected_columns = sorted(
        actual_types.keys() - EXPECTED_RAW_TYPES.keys()
    )

    type_mismatches = {
        name: {
            "expected": expected_type.simpleString(),
            "actual": actual_types[name].simpleString()
        }
        for name, expected_type in EXPECTED_RAW_TYPES.items()
        if name in actual_types
        and actual_types[name] != expected_type
    }

    if missing_columns or unexpected_columns or type_mismatches:
        raise ValueError(
            "Unexpected source schema: "
            f"missing={missing_columns}, "
            f"unexpected={unexpected_columns}, "
            f"type_mismatches={type_mismatches}"
        )
    

def validate_monetary_values(trips: DataFrame) -> None:
    checks = []

    for name in MONEY_COLUMNS:
        value = F.col(name)
        converted = F.round(value, 2).try_cast("decimal(18, 2)")

        invalid = value.isNotNull() & (
            F.isnan(value)
            | (F.abs(value) == F.lit(float("inf")))
            | converted.isNull()
        )

        checks.append(
            F.count(F.when(invalid, 1)).alias(name)
        )

    counts = trips.select(*checks).first().asDict()

    failures = {
        name: count
        for name, count in counts.items()
        if count > 0
    }

    if failures:
        raise ValueError(
            "Monetary values cannot safely become DECIMAL(18, 2): "
            f"{failures}"
        )


def transform_trips(
    trips: DataFrame,
    *,
    source_month: date,
    source_file: str,
    source_sha256: str,
) -> DataFrame:
    columns = []

    for source_name, silver_name in SOURCE_TO_SILVER.items():
        column = F.col(source_name)

        if source_name in MONEY_COLUMNS:
            column = F.round(column, 2).cast("decimal(18, 2)")

        columns.append(column.alias(silver_name))

    silver = trips.select(*columns)

    silver = silver.withColumns(
        {
            "pickup_date": F.to_date("pickup_datetime"),
            "duration_seconds": F.timestamp_diff(
                "SECOND",
                "pickup_datetime",
                "dropoff_datetime",
            ),
            "source_month": F.lit(source_month),
            "source_file": F.lit(source_file),
            "source_sha256": F.lit(source_sha256),
        }
    )

    return silver.withColumns(
        {
            "is_outside_source_month": (
                F.trunc("pickup_date", "month") != F.col("source_month")
            ),
            "has_missing_passenger_count": (
                F.col("passenger_count").isNull()
            ),
            "has_zero_passenger_count": (
                F.col("passenger_count") == 0
            ),
            "has_nonpositive_distance": (
                F.col("trip_distance") <= 0
            ),
            "has_nonpositive_duration": (
                F.col("dropoff_datetime") <= F.col("pickup_datetime")
            ),
            "has_negative_total_amount": (
                F.col("total_amount") < 0
            )
        }
    )


def create_silver_table(
    spark: SparkSession,
    silver: DataFrame,
    *,
    table_name: str = SILVER_TABLE,
) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

    if spark.catalog.tableExists(table_name):
        return

    (
        silver.limit(0)
        .writeTo(table_name)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .partitionedBy("source_month")
        .create()
    )


def write_silver_month(
    silver: DataFrame,
    *,
    source_month: date,
    table_name: str = SILVER_TABLE,
) -> None:
    silver.writeTo(table_name).overwrite(
        F.col("source_month") == F.lit(source_month)
    )


def load_silver_trips(
    spark: SparkSession,
    *,
    input_path: str,
    year: int,
    month: int,
    table_name: str = SILVER_TABLE,
) -> None:
    if not 1000 <= year <= 9999:
        raise ValueError("year must be a four-digit year")

    source_month = date(year, month, 1)

    source_sha256 = validate_source_path(
        input_path=input_path,
        year=year,
        month=month,
    )

    logger.info(
        "Starting Silver load: source_month=%s, input=%s, sha256=%s",
        source_month,
        input_path,
        source_sha256,
    )

    metadata = read_source_metadata(
        spark=spark,
        input_path=input_path,
    )

    validate_source_metadata(
        spark=spark,
        metadata=metadata,
        input_path=input_path,
        year=year,
        month=month,
        source_sha256=source_sha256,
    )

    logger.info(
        "Source metadata validated: filename=%s, size_bytes=%s",
        metadata["filename"],
        metadata["size_bytes"],
    )

    trips = spark.read.parquet(input_path)
    
    validate_source_schema(trips)
    validate_monetary_values(trips)

    silver = transform_trips(
        trips,
        source_month=source_month,
        source_file=input_path,
        source_sha256=source_sha256,
    )

    quality_flags = [
        "is_outside_source_month",
        "has_missing_passenger_count",
        "has_zero_passenger_count",
        "has_nonpositive_distance",
        "has_nonpositive_duration",
        "has_negative_total_amount",
    ]

    quality_counts = silver.select(
        *[
            F.count(F.when(F.col(name), 1)).alias(name)
            for name in quality_flags
        ]
    ).first()

    logger.info(
        "Silver quality checks: source_month=%s, count=%s",
        source_month,
        quality_counts.asDict(),
    )

    create_silver_table(
        spark,
        silver,
        table_name=table_name,
    )
    
    write_silver_month(
        silver,
        source_month=source_month,
        table_name=table_name,
    )

    logger.info(
        "Silver month committed: table=%s source_month=%s source_sha256=%s",
        table_name,
        source_month,
        source_sha256,
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Load TLC Yellow Taxi data into the Silver Iceberg table",
    )
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, choices=range(1, 13), required=True)
    args = parser.parse_args()

    # Reject an invalid path before starting Spark
    try:
        validate_source_path(
            input_path=args.input_path,
            year=args.year,
            month=args.month,
        )
    except ValueError as exc:
        parser.error(str(exc))

    spark = spark_session("load-silver-trips")

    try:
        load_silver_trips(
            spark,
            input_path=args.input_path,
            year=args.year,
            month=args.month,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()