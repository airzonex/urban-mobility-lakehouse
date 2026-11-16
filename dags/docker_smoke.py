from datetime import timedelta

import pendulum
from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator


with DAG(
    dag_id="docker_smoke",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["smoke"],
) as dag:
    run_container = DockerOperator(
        task_id="run_container",
        image="urban-mobility-ingestion:0.1.0",
        docker_url="unix://var/run/docker.sock",
        network_mode="urban-mobility-network",
        entrypoint=["python"],
        command=[
            "-c",
            (
                "import socket; "
                "print('MinIO address:', socket.gethostbyname('minio')); "
                "print('DOCKER_SMOKE_PASSED')"
            ),
        ],
        mount_tmp_dir=False,
        force_pull=False,
        auto_remove="success",
        do_xcom_push=False,
        mem_limit="256m",
        execution_timeout=timedelta(minutes=2),
        retries=0,
    )