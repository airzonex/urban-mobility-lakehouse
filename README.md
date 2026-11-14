# Urban Mobility Lakehouse

An educational data platform project based on urban mobility data. The goal is to build a lakehouse using Apache Spark, Apache Iceberg and S3-compatible object storage, with SQL access provided through Trino.

The data source is [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page): taxi trip records and a taxi zone lookup table. 

## Architecture

| Component | Purpose |
| --- | --- |
| Python | Source file ingestion and metadata generation |
| MinIO | Storage for source files, data files and Iceberg metadata |
| PostgreSQL | Iceberg JDBC catalog |
| Apache Spark / PySpark | Data validation, transformation and writing |
| Apache Iceberg | Management of analytical tables and snapshots |
| Trino | SQL access to Iceberg tables |
| Apache Airflow (planned) | Orchestration of data ingestion and transformations  |

Implemented layers: source files (Raw), normalized Iceberg tables (Silver) and analytical mart (Gold). Airflow orchestration is planned.

Silver retains suspicious records and marks them with quality flags. Invalid source metadata, unexpected schemas, empty source files and unrepresentable monetary values cause the load to fail before writing.

## Silver layer

The Spark job `jobs/load_silver_trips.py` loads one monthly TLC Yellow Taxi Parquet file from Raw storage into the Iceberg table `lakehouse.silver.yellow_trips`.

### Data contract

- One Silver row corresponds to one source record; duplicates are preserved.
- Column names are normalized to snake_case.
- Monetary values are rounded to two decimal places and stored as `DECIMAL(18, 2)`.
- Pickup and dropoff timestamps retain their source wall-clock values (`TIMESTAMP_NTZ`); they are not converted to UTC instants.
- Source provenance is stored in `source_month`, `source_file` and `source_sha256`.
- Suspicious records are retained and marked with quality flags.

### Validation

Before writing, the loader checks:

- The source URI matches the requested year, month, filename and SHA-256 layout.
- Metadata fields match the requested source.
- The Parquet object's size matches `size_bytes`.
- The source schema contains exactly the expected columns and types.
- The source contains at least one row.
- Non-null monetary values are finite and fit into `DECIMAL(18, 2)` after rounding.

The loader compares SHA-256 values in the URI and metadata. It does not recompute the Parquet hash during Silver loading.

### Monthly replacement

The table is partitioned by `source_month`.

Each successful load atomically replaces the requested source month. Other months remain unchanged. Repeating the same load does not duplicate rows, although it can create a new Iceberg snapshot.

An empty source is rejected to prevent accidental deletion of an existing month. Validation failures occur before the table write.

Run only one load at a time for a given source month.

### Loading

Upload the source file and metadata to Raw:

```bash
make ingest-month YEAR=2024 MONTH=1
```

Load the exact resulting object into Silver:

```bash
make load-silver YEAR=2024 MONTH=1 \
  INPUT_PATH='s3a://raw/tlc/yellow/year=2024/month=01/sha256=<hash>/yellow_tripdata_2024-01.parquet'
```

Replace `<hash>` with the ingestion result and adjust the bucket name if your configuration uses a name other than `raw`.

### Inspecting loaded data

Query the loaded months through Trino:

```sql
SELECT source_month, count(*) AS row_count
FROM lakehouse.silver.yellow_trips
GROUP BY source_month
ORDER BY source_month;
```

### Storage maintenance

Manage Iceberg tables through Spark or Trino. Do not manually delete their files from MinIO while retaining catalog entries.

PostgreSQL catalog metadata and MinIO warehouse objects must remain consistent when resetting or restoring the development environment.

## Gold layer

The Spark job `jobs/load_gold_daily_pickup_zone.py` aggregates Silver records into the Iceberg table `lakehouse.gold.daily_pickup_zone_stats`.

### Data contract

One row represents a combination of:

- `source_month`
- `pickup_date`
- `pickup_location_id`

The table contains these additive metrics:

| Columns | Meaning |
| --- | --- |
| `trip_count` | Number of eligible source records |
| `total_distance_miles` | Sum of trip distances |
| `total_duration_seconds` | Sum of trip durations on whole seconds |
| `total_amount_sum` | Sum of recorded total amounts, stored as `DECIMAL(28, 1)` |

`trip_count` counts source records, including duplicates retained in Silver. It does not guarantee unique trips.

`total_amount_sum` describes recorded amounts for eligible records; it is not defined as revenue.

### Inclusion rules

A record is included only when all these Silver flags are `false`:

- `is_outside_source_month`
- `has_nonpositive_duration`
- `has_nonpositive_distance`
- `has_negative_total_amount`

A `NULL` value in any of these flags also excludes the record.

Missing or zero passenger count do not exclude records because passenger count is not used in these metrics.

These rules define an analytical subset. Excluded records remain available in Silver.

### Monthly replacement

The table is partitioned by `source_month`.


Each load reads one Silver source month and atomically replaces that month's Gold aggregates. Other months remain unchanged.

- If the requested month has no Silver records, loading fails and existing Gold records remains unchanged.
- If Silver records exist but none meet the inclusion rules, previous Gold aggregates for that month are removed.
- Repeating a load wit unchanged input preserves logical results, although it may create a new Iceberg snapshot.

After reloading a Silver month, reload Gold for the same month.

Run only one Gold load at a time for a given source month.

### Loading

Load the requested month into Silver first, then run:

```bash
make load-gold-daily-pickup-zone YEAR=2024 MONTH=1
```

### Inspecting loaded data

Inspect the requested month's reconciliation through Trino:

```bash
make inspect-gold-daily-pickup-zone YEAR=2024 MONTH=1
```

Expected results:

- `trip_count_matches` is `true`
- `total_amount_matches` is `true`
- The duplicate-group query returns zero rows

The comparison uses the same inclusion rules for Silver as the Gold transformation.

This is an inspection command: a returned `false` does not automatically cause a nonzero exit status.

The requested month is substituted into the SQL template in memory. The tracked SQL file remains unchanged.

## Testing

Run the Spark integration tests:

```bash
make test-spark
```

These tests use controlled test data and isolated Iceberg tables to check transformations, validation, and monthly replacement behavior. They require the Docker-based Spark environment and its dependencies.

Tests do not verify the contents of your loaded production or demo tables. Use the layer-specific queries and reconciliation commands to inspect those datasets.