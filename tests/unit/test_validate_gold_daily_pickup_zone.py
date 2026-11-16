import importlib.util
import json
import subprocess
from pathlib import Path
from unittest.mock import Mock

import pytest

# load the standalone script without making scripts/ as Python package
SCRIPT_PATH = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "validate_gold_daily_pickup_zone.py"
)

spec = importlib.util.spec_from_file_location(
    "validate_gold_daily_pickup_zone",
    SCRIPT_PATH,
)
assert spec is not None
assert spec.loader is not None

validator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(validator)


@pytest.fixture
def run_trino(monkeypatch: pytest.MonkeyPatch) -> Mock:
    monkeypatch.setattr(
        "sys.argv",
        [
            "validate_gold_daily_pickup_zone",
            "--year", "2024",
            "--month", "1",
        ],
    )

    run = Mock()
    monkeypatch.setattr(validator.subprocess, "run", run)
    return run


@pytest.fixture
def passing_report() -> dict[str, bool]:
    return {
        "silver_month_exists": True,
        "trip_count_matches": True,
        "total_amount_matches": True,
        "grain_is_unique": True,
        "empty_subset_matches": True,
    }


def test_validation_passes_when_all_checks_are_true(
    run_trino: Mock,
    passing_report: dict[str, bool],
    capsys: pytest.CaptureFixture[str],
) -> None:
    run_trino.return_value = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=json.dumps(passing_report),
    )

    validator.main()

    assert "GOLD_VALIDATION_PASSED" in capsys.readouterr().out

    run_trino.assert_called_once()
    call = run_trino.call_args

    assert "{{source_month}}" not in call.kwargs["input"]
    assert "DATE '2024-01-01'" in call.kwargs["input"]
    assert call.kwargs["stdout"] == subprocess.PIPE

    command = call.args[0]
    format_index = command.index("--output-format")
    assert command[format_index + 1] == "JSON"


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
def test_validation_fails_when_a_check_is_false(
    run_trino: Mock,
    passing_report: dict[str, bool],
    capsys: pytest.CaptureFixture[str],
    check_name: str,
) -> None:
    passing_report[check_name] = False

    run_trino.return_value = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=json.dumps(passing_report),
    )

    with pytest.raises(SystemExit) as exc_info:
        validator.main()

    assert exc_info.value.code == 1

    captured = capsys.readouterr()
    assert check_name in captured.err
    assert "GOLD_VALIDATION_PASSED" not in captured.out


@pytest.mark.parametrize("invalid_value", [None, "true", 1])
def test_validation_requires_an_actual_boolean_true(
    run_trino: Mock,
    passing_report: dict[str, bool],
    invalid_value: object,
) -> None:
    report: dict[str, object] = dict(passing_report)
    report["trip_count_matches"] = invalid_value

    run_trino.return_value = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=json.dumps(report),
    )

    with pytest.raises(SystemExit) as exc_info:
        validator.main()

    assert exc_info.value.code == 1


def test_validation_fails_when_a_check_is_missing(
    run_trino: Mock,
    passing_report: dict[str, bool],
) -> None:
    del passing_report["trip_count_matches"]

    run_trino.return_value = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=json.dumps(passing_report),
    )

    with pytest.raises(SystemExit) as exc_info:
        validator.main()

    assert exc_info.value.code == 1


@pytest.mark.parametrize(
    "output",
    [
        "",
        "not JSON",
        "{}\n{}",
        "[]",
        "null",
    ],
)
def test_validation_rejects_invalid_result_output(
    run_trino: Mock,
    output: str,
) -> None:
    run_trino.return_value = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=output,
    )

    with pytest.raises(SystemExit) as exc_info:
        validator.main()

    assert exc_info.value.code == 1


def test_validation_fails_when_trino_fails(
    run_trino: Mock,
    passing_report: dict[str, bool],
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Even apparently successful output must not override a failed process.
    run_trino.return_value = subprocess.CompletedProcess(
        args=[],
        returncode=1,
        stdout=json.dumps(passing_report),
    )

    with pytest.raises(SystemExit) as exc_info:
        validator.main()

    assert exc_info.value.code == 1

    captured = capsys.readouterr()
    assert "validation query failed" in captured.err
    assert "GOLD_VALIDATION_PASSED" not in captured.out