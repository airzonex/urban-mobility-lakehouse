import json
import re
from datetime import timedelta

import pendulum
from airflow.sdk import DAG, Param, get_current_context, task
from airflow.providers.docker.operators.docker import DockerOperator


SPARK_ENVIRONMENT = {
    "PYSPARK_PYTHON": "python3",
    "SPARK_LOCAL_IP": "127.0.0.1",
    "CATALOG_JDBC_URL": (
        "jdbc:postgresql://"
        "{{ conn.iceberg_catalog.host }}:"
        "{{ conn.iceberg_catalog.port }}/"
        "{{ conn.iceberg_catalog.schema }}"
    ),
    "CATALOG_USER": "{{ conn.iceberg_catalog.login }}",
    "CATALOG_PASSWORD": "{{ conn.iceberg_catalog.password }}",
    "AWS_ACCESS_KEY_ID": "{{ conn.minio_raw.login }}",
    "AWS_SECRET_ACCESS_KEY": "{{ conn.minio_raw.password }}",
    "AWS_REGION": "{{ conn.minio_raw.extra_dejson.region_name }}",
    "AWS_EC2_METADATA_DISABLED": "true",
    "S3_ENDPOINT": "{{ conn.minio_raw.host }}",
    "ICEBERG_WAREHOUSE": "{{ conn.minio_raw.extra_dejson.warehouse }}",
}


@task(multiple_outputs=False)
def validate_ingestion_result(log_lines: list[str]) -> dict[str, str]:
    required_fields = {"status", "bucket", "object_key", "input_path"}
    candidates = []

    for line in log_lines:
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue

        if isinstance(value, dict) and required_fields.issubset(value):
            candidates.append(value)

    if len(candidates) != 1:
        raise ValueError(
            f"Expected one ingestion result, found {len(candidates)}"
        )

    result = candidates[0]

    for field in required_fields:
        if not isinstance(result[field], str) or not result[field]:
            raise ValueError(f"Invalid ingestion result field: {field}")

    if result["status"] not in ("uploaded", "skipped"):
        raise ValueError("Unexpected ingestion status")

    params = get_current_context()["params"]
    year = params["year"]
    month = params["month"]

    expected_key_pattern = (
        f"tlc/yellow/year={year}/month={month:02d}/"
        r"sha256=[0-9a-z]{64}/"
        rf"yellow_tripdata_{year}-{month:02d}\.parquet"
    )

    if re.fullmatch(expected_key_pattern, result["object_key"]) is None:
        raise ValueError("Object key does not match the requested month")

    expected_path = (
        f"s3a://{result["bucket"]}/{result["object_key"]}"
    )

    if result["input_path"] != expected_path:
        raise ValueError("Input path does not match bucket and object key")

    # return only the structured result for downstream tasks
    return {field: result[field] for field in sorted(required_fields)}


with DAG(
    dag_id="urban-mobility-monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "year": Param(2024, type="integer", minimum=1000, maximum=9999),
        "month": Param(1, type="integer", minimum=1, maximum=12),
    },
    tags=["urban-mobility"]
) as dag:
    ingest = DockerOperator(
        task_id="ingest",
        image="urban-mobility-ingestion:0.1.0",
        command=[
            "--year", "{{ params.year }}",
            "--month", "{{ params.month }}",
            "--json",
        ],
        docker_url="unix://var/run/docker.sock",
        network_mode="urban-mobility-network",
        environment={
            "S3_ENDPOINT": "{{ conn.minio_raw.host }}",
            "MINIO_ROOT_USER": "{{ conn.minio_raw.login }}",
            "MINIO_ROOT_PASSWORD": "{{ conn.minio_raw.password }}",
            "MINIO_REGION": "{{ conn.minio_raw.extra_dejson.region_name }}",
            "MINIO_RAW_BUCKET": "{{ conn.minio_raw.extra_dejson.bucket }}",
        },
        mount_tmp_dir=False,
        force_pull=False,
        auto_remove="success",
        do_xcom_push=True,
        xcom_all=True,
        mem_limit="1g",
        execution_timeout=timedelta(minutes=30),
        retries=0,
    )

    ingestion_result = validate_ingestion_result(ingest.output)

    load_silver = DockerOperator(
        task_id="load_silver",
        image="urban-mobility-spark:0.1.0",
        command=[
            "/opt/jobs/load_silver_trips.py",
            "--year", "{{ params.year }}",
            "--month", "{{ params.month }}",
            "--input-path",
            (
                "{{ ti.xcom_pull("
                "task_ids='validate_ingestion_result', "
                "key='return_value')['input_path'] }}"
            ),
        ],
        docker_url="unix://var/run/docker.sock",
        network_mode="urban-mobility-network",
        environment=SPARK_ENVIRONMENT.copy(),
        mount_tmp_dir=False,
        force_pull=False,
        auto_remove="success",
        do_xcom_push=False,
        mem_limit="2g",
        execution_timeout=timedelta(minutes=30),
        retries=0,
    )

    load_gold = DockerOperator(
        task_id="load_gold",
        image="urban-mobility-spark:0.1.0",
        command=[
            "/opt/jobs/load_gold_daily_pickup_zone.py",
            "--year", "{{ params.year }}",
            "--month", "{{ params.month }}",
        ],
        docker_url="unix://var/run/docker.sock",
        network_mode="urban-mobility-network",
        environment=SPARK_ENVIRONMENT.copy(),
        mount_tmp_dir=False,
        force_pull=False,
        auto_remove="success",
        do_xcom_push=False,
        mem_limit="2g",
        execution_timeout=timedelta(minutes=30),
        retries=0,
    )

    ingestion_result >> load_silver >> load_gold