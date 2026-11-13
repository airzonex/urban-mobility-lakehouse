.DEFAULT_GOAL := help

PYTHON := .venv/bin/python

.PHONY: help config up down ps logs build-spark \
		check-spark check-iceberg check-iceberg-read lint format \
		sql test-unit test-ingestion test-spark \
		ingest-month load-silver load-gold-daily-pickup-zone

help:
	@echo "config              Validate Compose configuration"
	@echo "up                  Start PostgreSQL and MinIO; initialize buckets"
	@echo "down                Stop services and preserve volumes"
	@echo "ps                  Show all project containers"
	@echo "logs                Show recent service logs"
	@echo "build-spark         Build the Spark image"
	@echo "check-spark         Run the basic Spark smoke check"
	@echo "check-iceberg       Write and check demo Iceberg table"
	@echo "check-iceberg-read  Check existing Iceberg table without writing"
	@echo "lint                Run Ruff checks"
	@echo "format              Format Python code with Ruff"
	@echo "test-unit		   Run unit tests"
	@echo "test-ingestion	   Run TLC ingestion tests"
	@echo "test-spark		   Run spark integration tests"

config:
	docker compose config --quiet

up:
	docker compose up -d postgres minio minio-init catalog-init trino

down:
	docker compose down

ps:
	docker compose ps -a

logs:
	docker compose logs --tail=100

build-spark:
	docker compose build spark catalog-init

check-spark:
	docker compose run --rm -T --no-deps spark /opt/jobs/hello_spark.py

check-iceberg:
	docker compose run --rm -T spark /opt/jobs/iceberg_smoke.py

check-iceberg-read:
	docker compose run --rm -T spark /opt/jobs/iceberg_smoke.py --read-only

lint:
	${PYTHON} -m ruff check jobs/

format:
	${PYTHON} -m ruff format jobs/

sql:
	docker compose exec trino trino --server http://localhost:8080 --catalog lakehouse --schema demo

test-unit:
	${PYTHON} -m pytest tests/unit -v

test-ingestion:
	${PYTHON} -m pytest tests/integration/ingestion -v

test-spark:
	docker compose run --rm \
		-v "$(CURDIR)/tests:/opt/tests:ro" \
		-e 'PYSPARK_SUBMIT_ARGS=--master local[2] --driver-memory 1g pyspark-shell' \
		--entrypoint /bin/bash \
		spark -c '\
			py4j_zip=(/opt/spark/python/lib/py4j-*-src.zip); \
			export PYTHONPATH="/opt/jobs:/opt/spark/python:$${py4j_zip[0]}$${PYTHONPATH:+:$$PYTHONPATH}"; \
			exec python3 -m pytest \
				/opt/tests/integration/spark \
				--import-mode=importlib \
				-o cache_dir=/tmp/pytest-cache \
				-v \
		'

ingest-month:
	@test -n "$(YEAR)" && test -n "$(MONTH)" || { echo "Usage: make ingest-month YEAR=2024 MONTH=1" >&2; exit 1; }
	$(PYTHON) -m urban_mobility.tlc_ingestion \
		--year "$(YEAR)" \
		--month "$(MONTH)"

load-silver:
	@test -n "$(YEAR)" && test -n "$(MONTH)" && test -n "$(INPUT_PATH)" || { \
		echo "Usage: make load-silver YEAR=2024 MONTH=1 INPUT_PATH='s3a://raw/tlc/yellow/year=2024/month=01/sha256=<sha256>/yellow_tripdata_2024-01.parquet'" >&2; \
		exit 1; \
	}
	docker compose run --rm spark \
		/opt/jobs/load_silver_trips.py \
		--year "$(YEAR)" \
		--month "$(MONTH)" \
		--input-path "$(INPUT_PATH)"

load-gold-daily-pickup-zone:
	@test -n "$(YEAR)" && test -n "$(MONTH)" || { echo "Usage: make load-gold-daily-pickup-zone YEAR=2024 MONTH=1" >&2; exit 1; }
	docker compose run --rm spark \
		/opt/jobs/load_gold_daily_pickup_zone.py \
		--year "$(YEAR)" \
		--month "$(MONTH)"