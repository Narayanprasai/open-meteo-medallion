-- Each location's actuals cover every hour between its first and last valid time (no missing rows).
select location_id, count(*) as n_rows,
       date_diff('hour', min(valid_time_utc), max(valid_time_utc)) + 1 as n_expected
from {{ ref('silver_actuals') }}
group by location_id
having count(*) <> date_diff('hour', min(valid_time_utc), max(valid_time_utc)) + 1
