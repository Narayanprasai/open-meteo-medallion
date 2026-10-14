/*
  Data completeness per source, model, lead day and variable, after dedup.
  Makes gaps in the upstream archives visible (e.g. ECMWF 17 to 22 April 2026).
*/
with deduped as (
    select *
    from {{ ref('stg_bronze__hourly_values') }}
    qualify row_number() over (
        partition by source, location_id, model, valid_time_utc, lead_day, variable
        order by (value is null), ingested_at_utc desc, request_id
    ) = 1
)

select
    source,
    model,
    coalesce(lead_day, 0)                                   as lead_day,
    variable,
    count(*)                                                as n_values,
    count(value)                                            as n_present,
    count(*) - count(value)                                 as n_missing,
    round(100.0 * count(value) / count(*), 3)               as pct_present,
    min(valid_time_utc)                                     as first_valid_time_utc,
    max(valid_time_utc) filter (where value is not null)    as last_present_time_utc
from deduped
group by all
