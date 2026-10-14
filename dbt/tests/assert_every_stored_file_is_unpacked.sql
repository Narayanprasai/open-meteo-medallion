-- Every file the ingestion log marks as stored must produce rows in staging.
-- Guards against silent emptiness (e.g. files moved, or dbt run from the wrong directory).
select log.request_id, log.source, log.file_path
from {{ source('bronze', 'ingestion_log') }} log
left join (select distinct request_id from {{ ref('stg_bronze__hourly_values') }}) stg using (request_id)
where log.status = 'stored' and stg.request_id is null
