/*
  Is the data fresh and complete? One row per bronze source.
  * last_ingested_at_utc: latest successful download
  * data_until_utc: latest hour with a real value (for actuals this is the ERA5 watermark: accuracy
    can only be scored up to here)
  * failed_last_7_days: failed requests in the ingestion log
  * min_pct_present: worst completeness across models, lead days and variables (see silver_coverage)
*/
with log as (
    select
        source,
        max(ingested_at_utc) filter (where status in ('stored', 'duplicate'))                 as last_ingested_at_utc,
        count(*) filter (where status = 'stored')                                              as files_stored,
        count(*) filter (where status = 'failed' and ingested_at_utc >= now()::timestamp - interval 7 day) as failed_last_7_days
    from {{ source('bronze', 'ingestion_log') }}
    group by source
),

coverage as (
    select source, max(last_present_time_utc) as data_until_utc, min(pct_present) as min_pct_present
    from {{ ref('silver_coverage') }}
    group by source
)

select
    log.source,
    log.last_ingested_at_utc,
    round(date_diff('minute', log.last_ingested_at_utc, now()::timestamp) / 60.0, 1) as hours_since_ingest,
    coverage.data_until_utc,
    log.files_stored,
    log.failed_last_7_days,
    coverage.min_pct_present
from log
left join coverage using (source)
