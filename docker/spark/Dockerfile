FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu

USER root

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl ca-certificates; \
    rm -rf /var/lib/apt/lists/*; \
    curl -fSL --retry 3 \
        https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.12.0/iceberg-spark-runtime-4.1_2.13-1.12.0.jar \
        -o /opt/spark/jars/iceberg-spark-runtime.jar; \
    curl -fSL --retry 3 \
        https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar \
        -o /opt/spark/jars/iceberg-aws-bundle.jar; \
    curl -fSL --retry 3 \
        https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar \
        -o /opt/spark/jars/postgresql.jar

USER 185

WORKDIR /opt/spark/work-dir

ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[2]", "--driver-memory", "1g"]