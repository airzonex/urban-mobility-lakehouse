# Daily pickup-zone statistics

## Target table

lakehouse.gold.daily_pickup_zone_stats

## Source

lakehouse.silver.yellow_trips

## Grain

One row per source_month, pickup_date and pickup_location_id.

source_month identifies the source batch and allows us to replace its aggregates when that batch is reloaded.

## Included records

Include records where:

- is_outside_source_month = false
- has_nonpositive_duration = false
- has_nonpositive_distance = false
- has_negative_total_amount = false

A missing or unknown value for any of these flags does not pass the filter.

Missing or zero passenger counts do not exclude a record: passenger count is not used in this table's metrics.

These rules define an analytical subset, not a claim that excluded records are necessarily erroneous.

## Columns

| Column | Meaning |
| --- | --- |
| source_month | Source batch month |
| pickup_date | Pickup calendar date |
| pickup_location_id | Pickup zone identifier |
| trip_count | Number of included source records |
| total_distance_miles | Sum of trip_distance |
| total_duration_seconds | Sum of duration_seconds |
| total_amount_sum | Sum of total_amount, using decimal arithmetic |

trip_count counts source records. It does not guarantee unique trips, because the source has no reliable trip identifier and Silver retains duplicates.

total_amount_sum represents recorded total amounts for included rows. It is not defined as revenue.

## Loading behavior

Recalculate and replace the complete source_month partition.

Reloading one month must not duplicate its aggregates or modify other months.

If the month has no qualifying records, remove any previous Gold aggregates for that month.

## Initial verification

- Each grouping key appears only once.
- The sum of trip_count equals the eligible Silver row count.
- The sum of total_amount_sum equals the eligible Silver amount sum.
- Reloading the same month produces the same logical results.
- Reloading one month leaves other months unchanged.