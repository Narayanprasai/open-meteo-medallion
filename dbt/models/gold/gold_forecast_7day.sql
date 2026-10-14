/*
  The 7 day outlook per location and model, in local calendar days, from the latest live forecast,
  with the expected error for that lead day taken from this location's own history.

  lead_day = local date minus the local date the forecast was issued (0 = today).
  Today and the last day are usually partial (the API window is in UTC); hours_covered shows it.
  Expected errors come from gold_daily_accuracy at the same lead (today uses lead 1, the closest scored).
*/
with hourly as (
    select
        f.location_id, f.model, f.issued_at_utc, l.timezone,
        {{ to_local('f.valid_time_utc', 'l.timezone') }}::date      as local_date,
        {{ to_local('f.issued_at_utc', 'l.timezone') }}::date       as issued_local_date,
        f.temperature_2m, f.precipitation, f.wind_speed_10m, f.relative_humidity_2m, f.weather_code
    from {{ ref('silver_forecast_current') }} f
    join {{ ref('dim_location') }} l using (location_id)
),

daily as (
    select
        location_id, model, local_date,
        max(issued_at_utc)                                       as issued_at_utc,
        (local_date - max(issued_local_date))::integer           as lead_day,
        count(*)                                                 as hours_covered,
        any_value({{ local_day_hours('local_date', 'timezone') }}) as hours_in_day,
        max(temperature_2m)                                      as temperature_max,
        min(temperature_2m)                                      as temperature_min,
        sum(precipitation)                                       as precipitation_sum,
        max(wind_speed_10m)                                      as wind_speed_max,
        avg(relative_humidity_2m)                                as relative_humidity_mean,
        max(weather_code)                                        as weather_code_max
    from hourly
    group by location_id, model, local_date
),

expected as (
    select location_id, model, lead_day,
           max(mae) filter (where metric = 'temperature_max') as expected_tmax_error,
           max(mae) filter (where metric = 'temperature_min') as expected_tmin_error
    from {{ ref('gold_daily_accuracy') }}
    where season = 'all'
    group by all
),

rain as (
    select location_id, model, lead_day, hit_rate, false_alarm_ratio
    from {{ ref('gold_rain_skill') }}
)

select
    d.location_id, l.name as location_name, l.state,
    d.model, d.local_date, d.lead_day, d.issued_at_utc,
    d.hours_covered, d.hours_in_day, d.hours_covered = d.hours_in_day as is_complete_day,
    round(d.temperature_max, 1)                                  as temperature_max,
    round(d.temperature_min, 1)                                  as temperature_min,
    round(d.precipitation_sum, 1)                                as precipitation_sum,
    d.precipitation_sum >= {{ var('rain_day_threshold_mm') }}    as rain_day_expected,
    round(d.wind_speed_max, 1)                                   as wind_speed_max,
    round(d.relative_humidity_mean, 0)                           as relative_humidity_mean,
    d.weather_code_max                                           as weather_code,
    wc.description                                               as weather_description,
    round(e.expected_tmax_error, 1)                              as expected_tmax_error,
    round(e.expected_tmin_error, 1)                              as expected_tmin_error,
    round(r.hit_rate, 2)                                         as rain_hit_rate_at_lead,
    round(r.false_alarm_ratio, 2)                                as rain_false_alarm_ratio_at_lead
from daily d
join {{ ref('dim_location') }} l using (location_id)
left join {{ ref('weather_codes') }} wc on wc.weather_code = d.weather_code_max
left join expected e on e.location_id = d.location_id and e.model = d.model
                    and e.lead_day = greatest(least(d.lead_day, 7), 1)
left join rain r     on r.location_id = d.location_id and r.model = d.model
                    and r.lead_day = greatest(least(d.lead_day, 7), 1)
where d.lead_day between 0 and 7
