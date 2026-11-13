import pytest
from load_silver_trips import MONEY_COLUMNS, validate_monetary_values
from pyspark.sql import SparkSession


@pytest.mark.parametrize(
    "invalid_value",
    [
        pytest.param(float("nan"), id="nan"),
        pytest.param(float("inf"), id="positive-infinity"),
        pytest.param(float("-inf"), id="negative-infinity"),
        pytest.param(1e20, id="decimal-overflow"),
    ]
)
def test_validate_monetary_values_rejects_invalid_amount(
    spark: SparkSession,
    invalid_value: float,
) -> None:
    values = {name: 0.0 for name in MONEY_COLUMNS}
    values["total_amount"] = invalid_value

    schema = ", ".join(
        f"`{name}` DOUBLE"
        for name in MONEY_COLUMNS
    )

    trips = spark.createDataFrame(
        [values],
        schema=schema
    )

    with pytest.raises(
        ValueError,
        match="Monetary values cannot safely become",
    ) as exc_info:
        validate_monetary_values(trips)

    assert "'total_amount': 1" in str(exc_info.value)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(12.50, id="positive"),
        pytest.param(0.0, id="zero"),
        pytest.param(-12.50, id="negative"),
        pytest.param(12.345, id="requires-rounding"),
        pytest.param(None, id="null"),
    ]
)
def test_validate_monetary_values_accepts_valid_values(
    spark: SparkSession,
    value: float | None,
) -> None:
    values = {name: value for name in MONEY_COLUMNS}

    schema = ", ".join(
        f"`{name}` DOUBLE"
        for name in MONEY_COLUMNS
    )

    trips = spark.createDataFrame(
        [values],
        schema=schema,
    )

    validate_monetary_values(trips)