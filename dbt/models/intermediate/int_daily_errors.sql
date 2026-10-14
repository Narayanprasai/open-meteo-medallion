/*
  Daily (local calendar day) forecast versus actual, one row per location, model, lead day and date.

  A "lead N" daily value aggregates the hourly lead N forecasts for that local day.
  Only complete days are kept: the day must have all its local hours (23, 24 or 25 with DST) for the
  actual and for every model. Partial days at the edges of the data are dropped, not guessed.
  Precipitation is accumulated over the preceding hour, so each hour is assigned to the local day of
  its valid (end) time.
*/
with hourly as (
    select
        f.location_id, f.model, f.lead_day,
        {{ to_local('f.valid_time_utc', 'l.timezone') }}::date as local_date,
        l.timezone,
        f.temperature_2m as fc_temp, f.precipitation as fc_precip, f.wind_speed_10m as fc_wind,
        a.temperature_2m as ac_temp, a.precipitation as ac_precip, a.wind_speed_10m as ac_wind
    from {{ ref('silver_forecast_by_lead') }} f
    join {{ ref('silver_actuals') }} a using (location_id, valid_time_utc)
    join {{ ref('dim_location') }} l using (location_id)
),

daily as (
    select
        location_id, model, lead_day, local_date,
        any_value({{ local_day_hours('local_date', 'timezone') }}) as expected_hours,
        count(*)                                                   as n_hours,
        count(fc_temp) filter (where ac_temp is not null)          as n_temp_pairs,
        count(fc_precip) filter (where ac_precip is not null)      as n_precip_pairs,
        count(fc_wind) filter (where ac_wind is not null)          as n_wind_pairs,
        max(fc_temp) as fc_tmax, min(fc_temp) as fc_tmin, sum(fc_precip) as fc_precip_sum, max(fc_wind) as fc_wind_max,
        max(ac_temp) as ac_tmax, min(ac_temp) as ac_tmin, sum(ac_precip) as ac_precip_sum, max(ac_wind) as ac_wind_max
    from hourly
    group by location_id, model, lead_day, local_date
),

complete as (
    select *
    from daily
    where n_hours = expected_hours
      and n_temp_pairs = expected_hours
      and n_precip_pairs = expected_hours
      and n_wind_pairs = expected_hours
    qualify count(*) over (partition by location_id, lead_day, local_date)
            = (select count(distinct model) from {{ ref('silver_forecast_by_lead') }})
)

select
    location_id, model, lead_day, local_date, expected_hours,
    {{ au_season('month(local_date)') }} as season,
    fc_tmax, ac_tmax, fc_tmin, ac_tmin, fc_precip_sum, ac_precip_sum, fc_wind_max, ac_wind_max,
    fc_precip_sum >= {{ var('rain_day_threshold_mm') }} as fc_rain_day,
    ac_precip_sum >= {{ var('rain_day_threshold_mm') }} as ac_rain_day
from complete
