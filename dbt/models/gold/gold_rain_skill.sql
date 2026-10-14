/*
  Will it rain? A rain day is a daily total of at least var('rain_day_threshold_mm').
  Contingency counts and standard scores per model and lead day, for all locations and per location:
  * hit rate (POD): share of actual rain days that were forecast
  * false alarm ratio (FAR): share of forecast rain days that stayed dry
  * critical success index (CSI): hits / (hits + misses + false alarms)
  * frequency bias: forecast rain days / actual rain days (above 1 means it predicts rain too often)
*/
with counts as (
    select
        model, lead_day,
        coalesce(location_id, 'ALL')                                   as location_id,
        count(*)                                                       as n_days,
        count(*) filter (where fc_rain_day and ac_rain_day)            as hits,
        count(*) filter (where not fc_rain_day and ac_rain_day)        as misses,
        count(*) filter (where fc_rain_day and not ac_rain_day)        as false_alarms,
        count(*) filter (where not fc_rain_day and not ac_rain_day)    as correct_dry
    from {{ ref('int_daily_errors') }}
    group by grouping sets ((model, lead_day), (model, lead_day, location_id))
)

select
    *,
    hits / nullif(hits + misses, 0)                          as hit_rate,
    false_alarms / nullif(hits + false_alarms, 0)            as false_alarm_ratio,
    hits / nullif(hits + misses + false_alarms, 0)           as critical_success_index,
    (hits + false_alarms) / nullif(hits + misses, 0)         as frequency_bias,
    (hits + correct_dry) / nullif(n_days, 0)                 as accuracy
from counts
