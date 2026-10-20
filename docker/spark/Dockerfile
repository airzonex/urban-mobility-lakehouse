FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu
WORKDIR /opt/spark/work-dir
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[2]", "--driver-memory", "1g"]