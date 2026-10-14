/*
  The most recent live forecast for each location, model and valid hour (UTC).
  issued_at_utc is when that forecast was downloaded. Includes the display only weather_code.
*/
with latest as (
    {{ latest_value_per_key('forecast', ['location_id', 'model', 'valid_time_utc', 'variable']) }}
)

select
    location_id,
    model,
    valid_time_utc,
    max(value) filter (where variable = 'temperature_2m')            as temperature_2m,
    max(value) filter (where variable = 'precipitation')             as precipitation,
    max(value) filter (where variable = 'wind_speed_10m')            as wind_speed_10m,
    max(value) filter (where variable = 'relative_humidity_2m')      as relative_humidity_2m,
    max(value) filter (where variable = 'weather_code')::integer     as weather_code,
    max(ingested_at_utc)                                             as issued_at_utc
from latest
group by all
