/*
  Hourly forecast versus actual, one row per location, model, lead day, variable and valid hour.

  Paired scoring: a row is kept only when the actual and EVERY model's forecast exist for that
  location, hour, lead day and variable. All models are therefore scored on identical hours, and
  upstream gaps (e.g. ECMWF 17 to 22 April 2026) remove that hour for every model, not just one.
*/
with forecasts as (
    select location_id, model, lead_day, valid_time_utc, variable, forecast
    from {{ ref('silver_forecast_by_lead') }}
    unpivot (forecast for variable in (temperature_2m, precipitation, wind_speed_10m, relative_humidity_2m))
),

actuals as (
    select location_id, valid_time_utc, variable, actual
    from {{ ref('silver_actuals') }}
    unpivot (actual for variable in (temperature_2m, precipitation, wind_speed_10m, relative_humidity_2m))
),

n_models as (
    select count(distinct model) as n from {{ ref('silver_forecast_by_lead') }}
),

joined as (
    select f.*, a.actual
    from forecasts f
    join actuals a using (location_id, valid_time_utc, variable)
    qualify count(*) over (partition by f.location_id, f.valid_time_utc, f.lead_day, f.variable)
            = (select n from n_models)
)

select
    j.location_id,
    j.model,
    j.lead_day,
    j.variable,
    j.valid_time_utc,
    {{ to_local('j.valid_time_utc', 'l.timezone') }}                   as valid_time_local,
    {{ to_local('j.valid_time_utc', 'l.timezone') }}::date             as local_date,
    {{ au_season('month(' ~ to_local('j.valid_time_utc', 'l.timezone') ~ ')') }} as season,
    j.forecast,
    j.actual,
    j.forecast - j.actual                                              as error
from joined j
join {{ ref('dim_location') }} l using (location_id)
