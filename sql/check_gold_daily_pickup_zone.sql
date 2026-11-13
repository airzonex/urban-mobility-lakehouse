-- compare Gold totals with eligible Silver records for the requested month
-- expected: both *_matches columns are true

WITH silver_totals AS (
    SELECT
        count(*) AS trip_count,
        coalesce(sum(total_amount), DECIMAL '0.00') AS total_amount
    FROM lakehouse.silver.yellow_trips
    WHERE source_month = DATE '{{source_month}}'
      AND NOT is_outside_source_month
      AND NOT has_nonpositive_distance
      AND NOT has_nonpositive_duration
      AND NOT has_negative_total_amount
),
gold_totals AS (
    SELECT
        coalesce(sum(trip_count), BIGINT '0') AS trip_count,
        coalesce(sum(total_amount_sum), DECIMAL '0.00') AS total_amount
    FROM lakehouse.gold.daily_pickup_zone_stats
    WHERE source_month = DATE '{{source_month}}'
)
SELECT
    s.trip_count AS silver_trip_count,
    g.trip_count AS gold_trip_count,
    s.total_amount AS silver_total_amount,
    g.total_amount AS gold_total_amount,
    s.trip_count = g.trip_count AS trip_count_matches,
    s.total_amount = g.total_amount AS total_amount_matches
FROM silver_totals s
CROSS JOIN gold_totals g;

-- verify the Gold grain
-- expected: zero rows

SELECT
    source_month,
    pickup_date,
    pickup_location_id,
    count(*) AS row_count
FROM lakehouse.gold.daily_pickup_zone_stats
WHERE source_month = DATE '{{source_month}}'
GROUP BY
    source_month,
    pickup_date,
    pickup_location_id
HAVING count(*) > 1;