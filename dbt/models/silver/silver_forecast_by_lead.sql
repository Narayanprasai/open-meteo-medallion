/*
  Forecasts made lead_day days before each valid hour (Previous Runs API), one row per
  location, model, valid hour and lead day. This is the input to every accuracy calculation.
  Times are UTC. Missing source values stay null (see silver_coverage).
*/
with latest as (
    {{ latest_value_per_key('previous_runs', ['location_id', 'model', 'valid_time_utc', 'lead_day', 'variable']) }}
)

select
    location_id,
    model,
    valid_time_utc,
    lead_day,
    max(value) filter (where variable = 'temperature_2m')       as temperature_2m,
    max(value) filter (where variable = 'precipitation')        as precipitation,
    max(value) filter (where variable = 'wind_speed_10m')       as wind_speed_10m,
    max(value) filter (where variable = 'relative_humidity_2m') as relative_humidity_2m,
    max(ingested_at_utc)                                        as ingested_at_utc
from latest
group by all
