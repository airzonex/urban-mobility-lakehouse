import os

from pyspark.sql import SparkSession


def spark_session(app_name: str) -> SparkSession:
    catalog = 'spark.sql.catalog.lakehouse'

    settings = {
        'spark.sql.extensions': (
            'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'
        ),
        'spark.sql.session.timeZone': 'UTC',
        'spark.sql.shuffle.partitions': '2',
        'spark.redaction.regex': (
            '(?i)secret|password|token|access[.]?key'
        ),
        catalog: 'org.apache.iceberg.spark.SparkCatalog',
        f'{catalog}.catalog-impl': 'org.apache.iceberg.jdbc.JdbcCatalog',
        f'{catalog}.uri': os.environ['CATALOG_JDBC_URL'],
        f'{catalog}.jdbc.user': os.environ['CATALOG_USER'],
        f'{catalog}.jdbc.password': os.environ['CATALOG_PASSWORD'],
        f'{catalog}.jdbc.schema-version': 'V1',
        f'{catalog}.warehouse': os.environ['ICEBERG_WAREHOUSE'],
        f'{catalog}.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
        f'{catalog}.s3.endpoint': os.environ['S3_ENDPOINT'],
        f'{catalog}.s3.path-style-access': 'true',
        f'{catalog}.client.region': os.environ['AWS_REGION'],
    }

    builder = SparkSession.builder.appName(app_name)

    for key, value in settings.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    return spark