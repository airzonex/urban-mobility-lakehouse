import importlib.util
import json
from pathlib import Path
from unittest.mock import patch

import pytest


DAG_PATH = (
    Path(__file__).resolve().parents[2]
    / "dags"
    / "urban_mobility_monthly.py"
)

spec = importlib.util.spec_from_file_location(
    "urban_mobility_monthly_ingestion_tests",
    DAG_PATH,
)
assert spec is not None
assert spec.loader is not None

monthly = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monthly)

validate_result = monthly.dag.get_task(
    "validate_ingestion_result"
).python_callable


@pytest.fixture(autouse=True)
def airflow_context():
    with patch.object(
        monthly,
        "get_current_context",
        return_value={"params": {"year": 2024, "month": 1}},
    ):
        yield


@pytest.fixture
def valid_result() -> dict[str, str]:
    object_key = (
        f"tlc/yellow/year=2024/month=01/sha256={'a' * 64}/"
        "yellow_tripdata_2024-01.parquet"
    )

    return {
        "status": "uploaded",
        "bucket": "raw",
        "object_key": object_key,
        "input_path": f"s3a://raw/{object_key}",
    }


@pytest.mark.parametrize("status", ["uploaded", "skipped"])
def test_accepts_valid_result_among_logs(
    valid_result: dict[str, str],
    status: str,
) -> None:
    valid_result["status"] = status

    lines = [
        "INFO Starting ingestion",
        '{"message": "unrelated JSON log"}',
        json.dumps(valid_result),
        "INFO Closing client",
    ]

    result = validate_result(lines)

    assert result == valid_result


@pytest.mark.parametrize(
    "lines",
    [
        [],
        ["INFO No result was emitted"],
        ['{"status":'],
    ],
)
def test_rejects_output_without_a_valid_result(
    lines: list[str],
) -> None:
    with pytest.raises(ValueError, match="Expected one ingestion result"):
        validate_result(lines)


def test_rejects_multiple_results(
    valid_result: dict[str, str],
) -> None:
    line = json.dumps(valid_result)

    with pytest.raises(ValueError, match="found 2"):
        validate_result([line, line])


@pytest.mark.parametrize(
    "missing_field",
    ["status", "bucket", "object_key", "input_path"],
)
def test_rejects_missing_field(
    valid_result: dict[str, str],
    missing_field: str,
) -> None:
    del valid_result[missing_field]

    with pytest.raises(ValueError, match="Expected one ingestion result"):
        validate_result([json.dumps(valid_result)])


@pytest.mark.parametrize("invalid_bucket", [None, "", 123])
def test_rejects_invalid_field_type_or_empty_value(
    valid_result: dict[str, str],
    invalid_bucket: object,
) -> None:
    result: dict[str, object] = dict(valid_result)
    result["bucket"] = invalid_bucket

    with pytest.raises(ValueError, match="Invalid ingestion result field: bucket"):
        validate_result([json.dumps(result)])


def test_rejects_unknown_status(
    valid_result: dict[str, str],
) -> None:
    valid_result["status"] = "failed"

    with pytest.raises(ValueError, match="Unexpected ingestion status"):
        validate_result([json.dumps(valid_result)])


def test_rejects_result_for_another_month(
    valid_result: dict[str, str],
) -> None:
    object_key = (
        f"tlc/yellow/year=2024/month=02/sha256={'a' * 64}/"
        "yellow_tripdata_2024-02.parquet"
    )

    valid_result["object_key"] = object_key
    valid_result["input_path"] = f"s3a://raw/{object_key}"

    with pytest.raises(ValueError, match="requested month"):
        validate_result([json.dumps(valid_result)])


def test_rejects_invalid_hash(
    valid_result: dict[str, str],
) -> None:
    object_key = valid_result["object_key"].replace(
        "a" * 64,
        "invalid-hash",
    )

    valid_result["object_key"] = object_key
    valid_result["input_path"] = f"s3a://raw/{object_key}"

    with pytest.raises(ValueError, match="requested month"):
        validate_result([json.dumps(valid_result)])


def test_rejects_inconsistent_input_path(
    valid_result: dict[str, str],
) -> None:
    valid_result["input_path"] = (
        f"s3a://another-bucket/{valid_result['object_key']}"
    )

    with pytest.raises(ValueError, match="Input path does not match"):
        validate_result([json.dumps(valid_result)])