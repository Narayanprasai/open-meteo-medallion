-- Every location, model and valid hour in silver_forecast_by_lead has exactly lead days 1 to 7.
select location_id, model, valid_time_utc, count(*) as n_leads, min(lead_day) as lo, max(lead_day) as hi
from {{ ref('silver_forecast_by_lead') }}
group by all
having count(*) <> 7 or min(lead_day) <> 1 or max(lead_day) <> 7
