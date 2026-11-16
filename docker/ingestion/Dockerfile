FROM python:3.10-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src/ ./src/

RUN python -m pip install .

RUN useradd --create-home --uid 10001 ingestion
USER ingestion

ENTRYPOINT ["python", "-m", "urban_mobility.tlc_ingestion"]