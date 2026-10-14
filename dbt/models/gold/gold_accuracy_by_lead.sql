/*
  How accurate is each model, and how does accuracy decay with lead time?
  Hourly scores per model, variable and lead day, for all locations ('ALL') and per location.
  Precipitation is scored as daily rain days in gold_rain_skill; its hourly MAE here is for reference.
*/
with scored as (
    select
        model, variable, lead_day,
        coalesce(location_id, 'ALL')            as location_id,
        count(*)                                as n_hours,
        avg(abs(error))                         as mae,
        avg(error)                              as bias,
        sqrt(avg(error * error))                as rmse,
        min(valid_time_utc)                     as scored_from_utc,
        max(valid_time_utc)                     as scored_until_utc
    from {{ ref('int_hourly_errors') }}
    group by grouping sets ((model, variable, lead_day), (model, variable, lead_day, location_id))
)

select
    s.*,
    -- growth of error relative to the 1 day ahead forecast of the same model, variable and location
    round(100 * (s.mae / first_value(s.mae) over (
        partition by s.model, s.variable, s.location_id order by s.lead_day) - 1), 1) as mae_increase_vs_day1_pct
from scored s
