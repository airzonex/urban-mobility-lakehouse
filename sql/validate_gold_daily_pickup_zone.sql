WITH silver_month AS (
    SELECT *
    FROM lakehouse.silver.yellow_trips
    WHERE source_month = DATE '{{source_month}}'
),
silver_totals AS (
    SELECT
        count(*) AS trip_count,
        coalesce(sum(total_amount), DECIMAL '0.00') AS total_amount
    FROM silver_month
    WHERE NOT is_outside_source_month
      AND NOT has_nonpositive_distance
      AND NOT has_nonpositive_duration
      AND NOT has_negative_total_amount
),
gold_month AS (
    SELECT *
    FROM lakehouse.gold.daily_pickup_zone_stats
    WHERE source_month = DATE '{{source_month}}'
),
gold_totals AS (
    SELECT
        count(*) AS group_count,
        coalesce(sum(trip_count), BIGINT '0') AS trip_count,
        coalesce(sum(total_amount_sum), DECIMAL '0.00') AS total_amount
    FROM gold_month
),
duplicate_groups AS (
    SELECT
        source_month,
        pickup_date,
        pickup_location_id
    FROM gold_month
    GROUP BY
        source_month,
        pickup_date,
        pickup_location_id
    HAVING count(*) > 1
),
validation_counts AS (
    SELECT
        (SELECT count(*) FROM silver_month) AS silver_row_count,
        (SELECT count(*) FROM duplicate_groups) AS duplicate_group_count
)
SELECT
    c.silver_row_count,
    s.trip_count AS silver_trip_count,
    g.trip_count AS gold_trip_count,
    s.total_amount AS silver_total_amount,
    g.total_amount AS gold_total_amount,
    g.group_count AS gold_group_count,
    c.duplicate_group_count,
    c.silver_row_count > 0 AS silver_month_exists,
    s.trip_count = g.trip_count AS trip_count_matches,
    s.total_amount = g.total_amount AS total_amount_matches,
    c.duplicate_group_count = 0 AS grain_is_unique,
    (
        s.trip_count > 0
        OR g.group_count = 0
    ) AS empty_subset_matches
FROM silver_totals s
CROSS JOIN gold_totals g
CROSS JOIN validation_counts c;