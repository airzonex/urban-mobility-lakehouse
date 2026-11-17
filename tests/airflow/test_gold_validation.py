import importlib.util
from pathlib import Path
from unittest.mock import Mock, patch

import pytest


DAG_PATH = (
    Path(__file__).resolve().parents[2] / "dags" / "urban_mobility_monthly.py"
)

spec = importlib.util.spec_from_file_location(
    "urban_mobility_monthly",
    DAG_PATH,
)
assert spec is not None
assert spec.loader is not None

monthly = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monthly)

# execute the task's Python function directly, without scheduling a task
validate_gold = monthly.dag.get_task("validate_gold").python_callable


@pytest.fixture
def passing_report() -> dict[str, bool]:
    return {
        "silver_month_exists": True,
        "trip_count_matches": True,
        "total_amount_matches": True,
        "grain_is_unique": True,
        "empty_subset_matches": True,
    }


@pytest.fixture
def trino_cursor():
    cursor = Mock()
    connection = Mock()
    connection.cursor.return_value = cursor

    with (
        patch.object(
            monthly,
            "get_current_context",
            return_value={"params": {"year": 2024, "month": 1}},
        ),
        patch.object(
            monthly.Path,
            "read_text",
            return_value="SELECT DATE '{{source_month}}';",
        ),
        patch.object(monthly, "TrinoHook") as hook,
    ):
        hook.return_value.get_conn.return_value = connection
        yield cursor


def configure_result(cursor: Mock, report: dict[str, object]) -> None:
    # DB-API column metadata: name followed by six metadata fields
    cursor.description = [
        (name, None, None, None, None, None, None)
        for name in report
    ]

    # first fetch returns the row; second confirms no additional rows
    cursor.fetchone.side_effect = [tuple(report.values()), None]


def test_gold_validation_accepts_passing_report(
    trino_cursor: Mock,
    passing_report: dict[str, bool],
) -> None:
    configure_result(trino_cursor, dict(passing_report))

    validate_gold()

    trino_cursor.execute.assert_called_once_with(
        "SELECT DATE '2024-01-01'"
    )
    trino_cursor.close.assert_called_once_with()


@pytest.mark.parametrize(
    "check_name",
    [
        "silver_month_exists",
        "trip_count_matches",
        "total_amount_matches",
        "grain_is_unique",
        "empty_subset_matches",
    ],
)
def test_gold_validation_rejects_failed_check(
    trino_cursor: Mock,
    passing_report: dict[str, bool],
    check_name: str,
) -> None:
    report: dict[str, object] = dict(passing_report)
    report[check_name] = False
    configure_result(trino_cursor, report)

    with pytest.raises(ValueError, match=check_name):
        validate_gold()


@pytest.mark.parametrize("invalid_value", [None, "true", 1])
def test_gold_validation_requires_boolean_true(
    trino_cursor: Mock,
    passing_report: dict[str, bool],
    invalid_value: object,
) -> None:
    report: dict[str, object] = dict(passing_report)
    report["trip_count_matches"] = invalid_value
    configure_result(trino_cursor, report)

    with pytest.raises(ValueError, match="trip_count_matches"):
        validate_gold()


def test_gold_validation_rejects_missing_check(
    trino_cursor: Mock,
    passing_report: dict[str, bool],
) -> None:
    report: dict[str, object] = dict(passing_report)
    del report["trip_count_matches"]
    configure_result(trino_cursor, report)

    with pytest.raises(ValueError, match="trip_count_matches"):
        validate_gold()