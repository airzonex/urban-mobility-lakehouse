import argparse
import subprocess
from datetime import date
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect Gold reconciliation for a source month"
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
    sql_path = project_root / "sql" / "check_gold_daily_pickup_zone.sql"

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
        ],
        input=sql,
        text=True,
        cwd=project_root,
        check=False,
    )

    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()