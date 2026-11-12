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

Implemented layers: source files (Raw) and normalized Iceberg tables (Silver). Analytical marts (Gold) and Airflow orchestration are planned.

Silver retains suspicious records and marks them with quality flags. Invalid source metadata, unexpected schemas, empty source files and unrepresentable monetary values cause the load to fail before writing.

## Silver layer

The Spark job `jobs/load_silver_trips.py` loads one monthly TLC Yellow Taxi Parquet file from Raw storage into the Iceberg table `lakehouse.silver.yellow_trips`.

### Data contract

- One Silver row corresponds to one source record; duplicates are preserved.
- Column names are normalized to snake_case.
- Monetary values are rounded to two decimal places and stored as `DECIMAL(18, 2)`.
- Pickup and dropoff timestamps retain their source wall-clock values (`TIMESTAMP_NTZ`); they are not converted to UTC instants.
- Source provenance is stored in `source_month`, `source_file` and `source_sha256`.
- Suspicious records are retained an marked with quality flags.

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
python -m urban_mobility.tlc_ingestion --year 2024 --month 1
```

Load the exact resulting object into Silver:

```bash
docker compose run --rm spark \
  /opt/jobs/load_silver_trips.py \
  --year 2024 \
  --month 1 \
  --input-path "s3a://raw/tlc/yellow/year=2024/month=01/sha256=<hash>/yellow_tripdata_2024-01.parquet"
```

Replace `<hash>` with the ingestion result and adjust the bucket name if your configuration uses a name other than `raw`.

### Verification

Run Spark integration tests:

```bash
make test-spark
```

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