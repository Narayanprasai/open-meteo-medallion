{{ config(materialized='table') }}

/*
  Every stored bronze value as one row: source, location, model, variable, lead day, valid hour (UTC).

  * Location: a bronze file is a list with one item per requested location, in request order.
    Item N maps to location_ids[N] from the ingestion log (the item itself carries no id).
  * Field names are <variable>[_previous_day<N>][_<model>]. The model suffix is matched against the
    models in the log row; a single model request (e.g. ERA5) has no suffix, so that model is used.
  * Only files logged as 'stored' are used, so stray files in data/bronze are ignored.
  * Files are matched to log rows on <source>/ingest_date=<d>/<name>.json (the name contains the
    payload hash), not the full path, so the match does not depend on where dbt is run from.
*/

{%- set sources = bronze_sources_with_files(['forecast', 'previous_runs', 'actuals']) %}

with log as (
    select request_id, source, location_ids, models, ingested_at_utc,
           regexp_extract(file_path, '[^/]+/ingest_date=[^/]+/[^/]+\.json$') as file_key
    from {{ source('bronze', 'ingestion_log') }}
    where status = 'stored'
),

unpacked as (
    {%- for name in sources %}
    select * from ({{ unpack_bronze_source(name) }})
    {%- if not loop.last %} union all by name {%- endif %}
    {%- endfor %}
    {%- if sources | length == 0 %}
    select null::varchar as source, null::varchar as file_path, null::bigint as item_index,
           null::varchar as valid_time, null::varchar as field, null::double as value
    where false
    {%- endif %}
),

joined as (
    select
        u.source, u.field, u.value, u.valid_time,
        log.location_ids[u.item_index] as location_id,
        log.models, log.request_id, log.ingested_at_utc
    from unpacked u
    join log on log.file_key = regexp_extract(u.file_path, '[^/]+/ingest_date=[^/]+/[^/]+\.json$')
            and log.source = u.source
),

with_model as (
    select
        *,
        coalesce(
            -- configured model whose name the field ends with (model names are not suffixes of each other)
            list_sort(list_filter(models, lambda m: ends_with(field, '_' || m)), 'DESC')[1],
            case when len(models) = 1 then models[1] end
        ) as model
    from joined
),

named as (
    select
        *,
        case when ends_with(field, '_' || model) then field[1:length(field) - length(model) - 1]
             else field end as base_name
    from with_model
)

select
    source,
    location_id,
    model,
    regexp_replace(base_name, '_previous_day\d+$', '')                       as variable,
    try_cast(regexp_extract(base_name, '_previous_day(\d+)$', 1) as integer)  as lead_day,
    strptime(valid_time, '%Y-%m-%dT%H:%M')                                    as valid_time_utc,
    value,
    request_id,
    ingested_at_utc
from named
