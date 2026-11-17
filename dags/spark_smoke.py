from datetime import timedelta

import pendulum
from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator


with DAG(
    dag_id="spark_smoke",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["smoke"],
) as dag:
    run_spark = DockerOperator(
        task_id="run_spark",
        image="urban-mobility-spark:0.1.0",
        command=["/opt/jobs/airflow_spark_smoke.py"],
        docker_url="unix://var/run/docker.sock",
        network_mode="urban-mobility-network",
        environment={
            "PYSPARK_PYTHON": "python3",
            "SPARK_LOCAL_IP": "127.0.0.1",
        },
        mount_tmp_dir=False,
        force_pull=False,
        auto_remove="success",
        do_xcom_push=False,
        mem_limit="2g",
        execution_timeout=timedelta(minutes=5),
        retries=0,
    )