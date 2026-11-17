import importlib.util
from pathlib import Path


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

dag = monthly.dag


def test_monthly_dag_has_expected_dependencies() -> None:
    expected_upstream = {
        "ingest": set(),
        "validate_ingestion_result": {"ingest"},
        "load_silver": {"validate_ingestion_result"},
        "load_gold": {"load_silver"},
        "validate_gold": {"load_gold"},
    }

    assert set(dag.task_ids) == set(expected_upstream)

    for task_id, upstream_ids in expected_upstream.items():
        assert dag.get_task(task_id).upstream_task_ids == upstream_ids


def test_monthly_dag_requires_upstream_success() -> None:
    for task in dag.tasks:
        assert task.trigger_rule == "all_success"


def test_gold_validation_is_the_only_leaf_task() -> None:
    assert {task.task_id for task in dag.leaves} == {"validate_gold"}


def test_monthly_dag_allows_only_one_active_run() -> None:
    assert dag.max_active_runs == 1