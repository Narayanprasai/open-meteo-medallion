-- Paired scoring: every model has the same number of scored hours per variable, lead day and location.
select variable, lead_day, location_id, min(n_hours) as lo, max(n_hours) as hi
from {{ ref('gold_model_comparison') }}
group by all
having min(n_hours) <> max(n_hours)
