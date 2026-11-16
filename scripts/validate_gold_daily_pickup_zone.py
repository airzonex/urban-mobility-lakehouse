import argparse
import json
import subprocess
import sys
from datetime import date
from pathlib import Path


CHECK_NAMES = (
    "silver_month_exists",
    "trip_count_matches",
    "total_amount_matches",
    "grain_is_unique",
    "empty_subset_matches",
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate Gold reconciliation for a source month"
    )
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    if not 1000 <= args.year <= 9999:
        parser.error("year must be a four-digit year")

    try:
        source_month = date(args.year, args.month, 1)
    except ValueError as error:
        parser.error(str(error))

    project_root = Path(__file__).resolve().parents[1]
    sql_path = project_root / "sql" / "validate_gold_daily_pickup_zone.sql"

    template = sql_path.read_text(encoding="utf-8")
    placeholder = "{{source_month}}"

    if placeholder not in template:
        raise ValueError(f"Missing {placeholder} placeholder in {sql_path}")

    sql = template.replace(placeholder, source_month.isoformat())

    result = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "trino",
            "trino",
            "--server",
            "http://localhost:8080",
            "--output-format",
            "JSON",
        ],
        input=sql,
        text=True,
        stdout=subprocess.PIPE,
        cwd=project_root,
        check=False,
    )

    if result.returncode != 0:
        print("ERROR: validation query failed", file=sys.stderr)
        raise SystemExit(1)

    try:
        report = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        print(
            f"ERROR: expected not JSON result row: {error}",
            file=sys.stderr,
        )
        raise SystemExit(1) from error

    if not isinstance(report, dict):
        print("ERROR: expected a JSON object", file=sys.stderr)
        raise SystemExit(1)

    print(f"Gold validation for {source_month.isoformat()}")
    print(result.stdout.strip())

    failed_checks = [
        name for name in CHECK_NAMES
        if report.get(name) is not True
    ]

    if failed_checks:
        print(
            f"ERROR: failed checks: {', '.join(failed_checks)}",
            file=sys.stderr,
        )
        raise SystemExit(1)

    print("GOLD_VALIDATION_PASSED")


if __name__ == "__main__":
    main()