FROM apache/airflow:3.3.1-python3.12

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow==3.3.1" \
    apache-airflow-providers-docker \
    apache-airflow-providers-trino \
    --constraint "${HOME}/constraints.txt"

RUN pip check