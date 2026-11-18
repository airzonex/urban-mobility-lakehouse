# Urban Mobility Lakehouse

An educational data platform project based on urban mobility data. The goal is to build a lakehouse using Apache Spark, Apache Iceberg and S3-compatible object storage, with SQL access provided through Trino.

The data source is [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). The current pipeline processes monthly Yellow Taxi trip records. Taxi zone lookup integration is planned.

## Architecture

| Component | Purpose |
| --- | --- |
| Python | Source file ingestion and metadata generation |
| MinIO | Storage for source files, data files and Iceberg metadata |
| PostgreSQL | Separate databases and services for the Iceberg JDBC catalog and Airflow metadata |
| Apache Spark / PySpark | Data validation, transformation and writing |
| Apache Iceberg | Management of analytical tables and snapshots |
| Trino | SQL access to Iceberg tables |
| Apache Airflow | Orchestration of data ingestion and transformations  |

Implemented layers: Raw source files, Silver Iceberg tables, and Gold daily pickup-zone statistics. A manually triggered Airflow DAG orchestrates monthly loading and reconciliation.

Silver retains suspicious records and marks them with quality flags. Invalid source metadata, unexpected schemas, empty source files and unrepresentable monetary values cause the load to fail before writing.

Airflow uses LocalExecutor. DockerOperator tasks run in scheduler subprocesses and ask the host Docker daemon to launch separate ingestion and Spark containers.

Spark jobs run with `local[2]` inside a single container; this setup does not deploy a distributed Spark cluster.

Gold validation runs in an Airflow task and queries Trino directly.

## First-time setup

Run the following commands from the project root.

### Prerequisites

Install:

- Docker Engine and Docker Compose with support for `docker compose up --wait`.
- GNU Make.
- Python 3.10 for ingestion development and unit tests.
- Python 3.12 for local Airflow development and tests.

The Airflow Docker setup expects a Linux host with Docker available at `/var/run/docker.sock`. The scheduler uses this socket to start ingestion and Spark containers. This grants it extensive control over the host Docker daemon, so use this configuration only in a trusted development environment.

### Configure the environment

Create your local environment file:

```bash
cp .env.example .env
```

Review `.env` and fill in the MinIO credentials, catalog database settings, and Airflow database settings. Keep `.env` out of version control.

Create the directories used by Airflow:

```bash
mkdir -p airflow/logs airflow/config
```

Find your user ID and the Docker socket group ID:

```bash
id -u
stat -c '%g' /var/run/docker.sock
```

Set `AIRFLOW_UID` and `DOCKER_GID` in `.env` to these values.

Generate separate secrets for the Airflow database password and JWT signing key:

```bash
openssl rand -hex 24
openssl rand -hex 32
```

Use the first value for `AIRFLOW_DB_PASSWORD` and the second for `AIRFLOW_JWT_SECRET`.

### Build the application images

Build the Airflow image first, without loading the Compose configuration:

```bash
docker build \
  -t urban-mobility-airflow:0.1.0 \
  docker/airflow
```

Generate an Airflow Fernet key using the built image:

```bash
docker run --rm \
  --entrypoint python \
  urban-mobility-airflow:0.1.0 \
  -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'
```

Save the output as `AIRFLOW_FERNET_KEY` in `.env`. Keep this key stable: Airflow uses it to encrypt stored connection credentials.

With all required environment variables configured, build the ingestion and Spark images:

```bash
docker compose build ingestion spark
```

### Start the services

```bash
make up
```

This starts the storage and query services, migrates the Airflow metadata database, and starts the Airflow API server, scheduler, and DAG processor.

Check the service status:

```bash
make ps
```

Open Airflow at <http://localhost:18081>, or use the port configured by `AIRFLOW_PORT`.

Sign in as `admin`. The generated password is stored in:

```text
airflow/config/simple_auth_manager_passwords.json
```

Create the `minio_raw`, `iceberg_catalog`, and `trino_lakehouse` connections using the settings in the Airflow section before triggering the monthly DAG.

### Run the first month

Unpause `urban_mobility_monthly` in the Airflow UI, then trigger it manually with:

```json
{
  "year": 2024,
  "month": 1
}
```

Wait for all five tasks to succeed. The final `validate_gold` task checks the loaded month and logs `GOLD_VALIDATION_PASSED` when every validation check passes.

### Local development

Use separate Python environments for the ingestion code and Airflow.

Create the project environment and install the package with development dependencies:

```bash
python3.10 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -e ".[dev]"
```

Create the Airflow environment:

```bash
python3.12 -m venv .venv-airflow
.venv-airflow/bin/python -m pip install --upgrade pip
```

Install Airflow, its providers, and pytest using the constraints matching the container's Airflow and Python versions:

```bash
.venv-airflow/bin/python -m pip install \
  "apache-airflow==3.3.1" \
  apache-airflow-providers-docker \
  apache-airflow-providers-trino \
  pytest \
  --constraint \
  "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt"
```

Check dependency compatibility:

```bash
.venv/bin/python -m pip check
.venv-airflow/bin/python -m pip check
```

Run the unit tests and Airflow tests:

```bash
make test-unit
make test-airflow
```

The Makefile uses `.venv/bin/python` and `.venv-airflow/bin/python` explicitly, so activating either environment is unnecessary.

See the Testing section for integration test requirements.

### Stop the services

After active DAG runs finish, stop the services:

```bash
make down
```

Named volumes are retained, preserving stored data and Airflow metadata. Avoid `docker compose down -v` unless you intend to delete those volumes.

Containers started by Airflow's DockerOperator are managed separately from the Compose services.

## Raw layer

The ingestion job downloads a monthly TLC Yellow Taxi Parquet file, calculates its SHA-256 hash, and stores the file with accompanying metadata in MinIO.

Data objects use this layout:

`tlc/yellow/year=YYYY/month=MM/sha256=<sha256>/yellow_tripdata_YYYY-MM.parquet`

The content hash distinguishes different versions of a source file for the same month.

Ingestion returns a structured result containing `status`, `bucket`, `object_key` and `input_path`. The CLI exposes this result through the optional `--json` argument.

- `uploaded`: data and metadata were uploaded, including repair cases.
- `skipped`: existing objects passed the reuse checks, so uploading was skipped.

The source is still downloaded to calculate its hash before deciding whether an upload can be skipped.

Airflow passes the exact returned object path to Silver.

### Ingestion metadata

Each ingested file has a companion `metadata.json` object in the same content-addressed prefix:

```text
tlc/yellow/year=2024/month=01/sha256=<sha256>/yellow_tripdata_2024-01.parquet
tlc/yellow/year=2024/month=01/sha256=<sha256>/metadata.json
```

Both objects are stored in the bucket configured by `MINIO_RAW_BUCKET`. `<sha256>` is the downloaded file's full SHA-256 checksum.

The metadata object contains:

| Field | JSON type | Description |
|---|---|---|
| `source_url` | string | URL used to download the source Parquet file. |
| `year` | integer | Requested source year. |
| `month` | integer | Requested source month, from 1 to 12. |
| `filename` | string | Source filename, such as `yellow_tripdata_2024-01.parquet`. |
| `sha256` | string | SHA-256 checksum of the downloaded bytes, expressed as 64 hexadecimal characters. |
| `size_bytes` | integer | Size of the downloaded file in bytes. |
| `downloaded_at` | string | UTC timestamp recorded after the download completes, in ISO 8601 format. |

The Parquet file is uploaded first, followed by the metadata object.

On a repeated run, ingestion downloads the source again and calculates its checksum and size. If the existing metadata matches the source details, `downloaded_at` is a nonempty string, and the stored Parquet object has the
expected size, the upload is skipped.

Missing or invalid metadata, a missing Parquet object, or a size mismatch causes ingestion to upload the data and metadata again. Existing invalid metadata is removed before repair.

When an upload is skipped, the existing metadata remains unchanged. Its `downloaded_at` therefore describes the download associated with the stored upload, rather than the latest ingestion attempt.

The stored Parquet object is checked by size; ingestion does not download it again from MinIO to verify its checksum.

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

Use the complete `input_path` returned by ingestion as `INPUT_PATH`. The example above illustrates its format.

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

| Column | Meaning |
| --- | --- |
| `trip_count` | Number of eligible source records |
| `total_distance_miles` | Sum of trip distances |
| `total_duration_seconds` | Sum of trip durations in whole seconds |
| `total_amount_sum` | Sum of recorded total amounts, stored as `DECIMAL(28, 2)` |

`trip_count` counts source records, including duplicates retained in Silver. It does not guarantee unique trips.

`total_amount_sum` describes recorded amounts for eligible records; it is not defined as revenue.

### Inclusion rules

A record is included only when all these Silver flags are `false`:

- `is_outside_source_month`
- `has_nonpositive_duration`
- `has_nonpositive_distance`
- `has_negative_total_amount`

A `NULL` value in any of these flags also excludes the record.

Missing or zero passenger counts do not exclude records because passenger count is not used in these metrics.

These rules define an analytical subset. Excluded records remain available in Silver.

### Monthly replacement

The table is partitioned by `source_month`.


Each load reads one Silver source month and atomically replaces that month's Gold aggregates. Other months remain unchanged.

- If the requested month has no Silver records, loading fails and existing Gold data remains unchanged.
- If Silver records exist but none meet the inclusion rules, previous Gold aggregates for that month are removed.
- Repeating a load with unchanged input preserves logical results, although it may create a new Iceberg snapshot.

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

### Automated validation

After loading Gold, validate the requested source month:

```bash
make validate-gold-daily-pickup-zone YEAR=2024 MONTH=1
```

The command checks that:

- Silver contains records for the requested source month.
- Gold's summed trip count matches the number of eligible Silver records.
- Gold's summed total amount matches the sum of `total_amount` over eligible Silver records.
- Gold contains no duplicate combinations of source month, pickup date and pickup location.
- Gold contains no rows when Silver exists but no records qualify.

The command exits successfully only when every check passes.
Query execution errors, invalid result output and failed checks produce a nonzero exit status.

Unlike `inspect-gold-daily-pickup-zone`, this command interprets the results and can fail an automated pipeline.

Validation reads existing tables without modifying them. Run it after the month's loading jobs finish, with no concurrent writes to that month.

These checks verify monthly totals and grain uniqueness. They do not verify every individual pickup-date/location aggregate.

## Airflow orchestration

The `urban_mobility_monthly` DAG loads one requested source month:

1. `ingest` — downloads the source file and uploads or reuses Raw objects.
2. `validate_ingestion_result` — validates the returned object path.
3. `load_silver` — replaces the requested Silver month.
4. `load_gold` — replaces the requested Gold month.
5. `validate_gold` — reconciles Gold against eligible Silver records.

An ingestion result of `skipped` means the Raw upload was skipped. Silver, Gold, and validation still run.

### Starting Airflow

With `.env` configured and the job images built, run:

```bash
make up
```

This starts the infrastructure, runs Airflow metadata migrations and starts the API server, scheduler, and DAG processor.

Open http://localhost:18081, or the port configured by `AIRFLOW_PORT`.

For the local Simple Auth Manager setup, the username is `admin`. Read its generated password from:

```bash
cat airflow/config/simple_auth_manager_passwords.json
```

### Required connections

Create these connections in the Airflow UI:

| Connection ID | Type | Host | Port | Login / password | Schema |
| --- | --- | --- | --- | --- | --- |
| `minio_raw` | Generic | `http://minio:9000` | Leave empty | MinIO credentials | Leave empty |
| `iceberg_catalog` | Generic | `postgres` | `5432` | Iceberg catalog credentials | Catalog database name |
| `trino_lakehouse` | Trino | `trino` | `8080` | Login `airflow`; no password for this local setup | `gold` |

Extra for `minio_raw`, adjusted to match `.env`:

```json
{
  "region_name": "us-east-1",
  "bucket": "raw",
  "warehouse": "s3://warehouse"
}
```

Extra for `trino_lakehouse`:

```json
{
  "catalog": "lakehouse",
  "protocol": "http"
}
```

The `iceberg_catalog` connection points to the Iceberg catalog database, not Airflow's metadata database.

### Loading a month

Unpause `urban_mobility_monthly` and trigger it with:

```json
{
  "year": 2024,
  "month": 1
}
```

The DAG has no automatic schedule and allows one active run at a time.

A successful run requires all five tasks to succeed. The final task logs:

```text
GOLD_VALIDATION_PASSED
```

Validation checks monthly totals, Gold grain uniqueness, source-month presence, and correct handling of an empty eligible subset. It does not compare every individual Gold aggregate with Silver.

Avoid running manual loading commands concurrently with DAG writes to the same table and source month.

### Retries and recovery

| Task | Additional retries | Delay |
| --- | ---: | --- |
| `ingest` | 2 | 1 minute |
| `validate_ingestion_result` | 0 | — |
| `load_silver` | 1 | 2 minutes |
| `load_gold` | 1 | 2 minutes |
| `validate_gold` | 0 | — |

Execution timeouts apply separately to each attempt.

After correcting a failure, clear the failed task in the same DAG run. Include downstream tasks when they also need to execute again and review the selected task instances before confirming.

If Silver is rerun, rerun Gold and final validation afterward. If Gold is rerun, rerun final validation afterward.

For a temporary Trino outage after successful loading, restore the connection and clear only `validate_gold`.

Validation failure does not roll back previously committed data.

If the scheduler crashes during a Docker job, check for a surviving job container before retrying a writer.

### Rebuilding images

After changing ingestion code:

```bash
docker compose build ingestion
```

After changing Spark jobs or their dependencies:

```bash
docker compose build spark
```

After changing the Airflow Dockerfile or installed providers:

```bash
docker compose build airflow-init
```

Recreate the running Airflow services to use the rebuilt image:

```bash
docker compose up -d --force-recreate --wait \
  airflow-api-server \
  airflow-scheduler \
  airflow-dag-processor
```

Perform Airflow service recreation when no DAG run is active.

DAG files and validation SQL are mounted from the repository. Editing them does not require rebuilding an image. Allow the DAG processor time to discover DAG changes.

### Troubleshooting

Check DAG import errors:

```bash
docker compose exec -T airflow-dag-processor \
  airflow dags list-import-errors
```

Inspect service logs:

```bash
docker compose logs --tail=100 \
  airflow-api-server \
  airflow-scheduler \
  airflow-dag-processor
```

Inspect individual task logs in the Airflow UI.

The local scheduler uses the host Docker socket to launch job containers. This grants powerful host access and assumes trusted DAG code.

## Testing

### Unit tests

Run all unit tests:

```bash
make test-unit
```

These tests check ingestion logic, CLI behavior, and the Gold validation wrapper using mocked external dependencies. They require the project's Python environment and development dependencies, but do not require
Docker, MinIO, or Trino.

### Ingestion integration tests

Run the ingestion integration tests:

```bash
make test-ingestion
```

These tests exercise the ingestion pipeline against a running MinIO instance. They use temporary buckets that the test fixtures clean up afterward.

### Spark integration tests

Run the Spark integration tests:

```bash
make test-spark
```

These tests use controlled test data and isolated Iceberg tables to check transformations, validation, and monthly replacement behavior.
They require the Docker-based Spark environment and its dependencies.

### Airflow tests

Run the orchestration tests:

```bash
make test-airflow
```

These tests require `.venv-airflow` with Airflow, its Docker and Trino providers and pytest installed.

They check DAG dependencies, concurrency configuration, ingestion-result parsing, and Gold validation decisions. External calls are mocked; running Docker services are not required.

The AnyIO pytest plugin is disabled for this synchronous test suite.

Automated tests do not verify the contents of your loaded production or demo tables. Use the layer-specific inspection and validation commands to check those datasets.

## Verification

The project was installed and tested on a clean Ubuntu VM using the first-time setup instructions.

Verified scenarios:

- Unit, ingestion integration, Spark integration, and Airflow tests pass.
- The monthly Airflow pipeline completes successfully.
- Reprocessing January preserves Silver row counts and Gold row counts, trip counts, and monetary totals.
- Loading February preserves January's recorded counts and totals.
- Gold validation passes for both months.
- After a simulated Trino connection failure, clearing only `validate_gold` recovers the same DAG run without rerunning successful upstream tasks.

The repeatability and month-isolation checks compare counts and monetary totals; they are not full row-by-row comparisons.