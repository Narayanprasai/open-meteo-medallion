/*
  Which model is better? Both models are scored on identical hours (paired, see int_hourly_errors).
  Per variable and lead day, for all locations and per location:
  * mae and rank (1 = most accurate)
  * mae_gap_to_best: how much worse than the best model (0 for the best)
  * share_hours_closest: share of hours where this model was the closer of the models (ties split)
  Caveat: truth is ERA5, produced by ECMWF on the same grid as ecmwf_ifs025 (see README).
*/
with ranked_hours as (
    select
        *,
        abs(error) as abs_error,
        min(abs(error)) over (partition by location_id, variable, lead_day, valid_time_utc) as best_abs_error,
        count(*) filter (where true) over (partition by location_id, variable, lead_day, valid_time_utc) as n_models
    from {{ ref('int_hourly_errors') }}
),

hour_share as (
    select
        *,
        -- 1 if this model alone was closest; split evenly when models tie
        (abs_error = best_abs_error)::double
            / count(*) filter (where true) over (
                partition by location_id, variable, lead_day, valid_time_utc, (abs_error = best_abs_error))
            * (abs_error = best_abs_error)::integer as closest_share
    from ranked_hours
),

scored as (
    select
        model, variable, lead_day,
        coalesce(location_id, 'ALL')        as location_id,
        count(*)                            as n_hours,
        avg(abs_error)                      as mae,
        sum(closest_share) / count(*)       as share_hours_closest
    from hour_share
    group by grouping sets ((model, variable, lead_day), (model, variable, lead_day, location_id))
)

select
    *,
    rank() over (partition by variable, lead_day, location_id order by mae)                 as rank,
    mae - min(mae) over (partition by variable, lead_day, location_id)                      as mae_gap_to_best
from scored
