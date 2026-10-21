.DEFAULT_GOAL := help

PYTHON := .venv/bin/python

.PHONY: help config up down ps logs build-spark \
		check-spark check-iceberg check-iceberg-read lint format \
		sql

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

config:
	docker compose config --quiet

up:
	docker compose up -d postgres minio minio-init trino

down:
	docker compose down

ps:
	docker compose ps -a

logs:
	docker compose logs --tail=100

build-spark:
	docker compose build spark

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