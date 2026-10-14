/*
  Accuracy of what people actually read: daily max and min temperature, daily rain total and peak wind,
  per local calendar day (DST aware). Per model and lead day, for all locations and per location.
*/
with long as (
    select location_id, model, lead_day, season, 'temperature_max' as metric, fc_tmax - ac_tmax as error from {{ ref('int_daily_errors') }}
    union all
    select location_id, model, lead_day, season, 'temperature_min', fc_tmin - ac_tmin from {{ ref('int_daily_errors') }}
    union all
    select location_id, model, lead_day, season, 'precipitation_sum', fc_precip_sum - ac_precip_sum from {{ ref('int_daily_errors') }}
    union all
    select location_id, model, lead_day, season, 'wind_speed_max', fc_wind_max - ac_wind_max from {{ ref('int_daily_errors') }}
)

select
    model, metric, lead_day,
    coalesce(location_id, 'ALL')    as location_id,
    coalesce(season, 'all')         as season,
    count(*)                        as n_days,
    avg(abs(error))                 as mae,
    avg(error)                      as bias,
    sqrt(avg(error * error))        as rmse,
    -- share of days within 2 units (2 °C for temperature) of the actual
    avg((abs(error) <= 2)::integer) as share_within_2
from long
group by grouping sets (
    (model, metric, lead_day),
    (model, metric, lead_day, location_id),
    (model, metric, lead_day, season)
)
