/*
  Observed weather (truth) from ERA5 reanalysis, one row per location and valid hour, UTC.
  The most recent hours are null until ERA5 publishes them (~6 day lag); later ingestion runs fill them.
*/
with latest as (
    {{ latest_value_per_key('actuals', ['location_id', 'valid_time_utc', 'variable']) }}
)

select
    location_id,
    any_value(model)                                            as truth_model,
    valid_time_utc,
    max(value) filter (where variable = 'temperature_2m')       as temperature_2m,
    max(value) filter (where variable = 'precipitation')        as precipitation,
    max(value) filter (where variable = 'wind_speed_10m')       as wind_speed_10m,
    max(value) filter (where variable = 'relative_humidity_2m') as relative_humidity_2m,
    max(ingested_at_utc)                                        as ingested_at_utc
from latest
group by location_id, valid_time_utc
