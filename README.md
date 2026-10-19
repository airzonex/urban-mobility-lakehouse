[Русская версия](README.ru.md)

# Urban Mobility Lakehouse

An educational data platform project based on urban mobility data. The goal is to build a lakehouse using Apache Spark, Apache Iceberg and S3-compatible object storage, with SQL access provided through Trino.

The planned data source is [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page): taxi trip records and a taxi zone lookup table. 

## Planned architecture

| Component | Purpose |
| --- | --- |
| Python | Source data ingestion and ingestion run tracking |
| MinIO | Storage for source files, data files and Iceberg metadata |
| PostgreSQL | Iceberg JDBC catalog |
| Apache Spark / PySpark | Data validation, transformation and writing |
| Apache Iceberg | Management of analytical tables and snapshots |
| Trino | SQL access to Iceberg tables |
| Apache Airflow | Orchestration o data ingestion and transformations  |

Planned layers: source files (Raw), normalized tables (Silver) and analytical marts (Gold). Records that fail data quality checks will be stored separately.
