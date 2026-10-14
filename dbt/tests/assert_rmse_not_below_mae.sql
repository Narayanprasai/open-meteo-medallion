-- Mathematical sanity: RMSE is always >= MAE (small tolerance for floating point).
select * from {{ ref('gold_accuracy_by_lead') }} where rmse < mae - 1e-9
union all by name
select * from {{ ref('gold_daily_accuracy') }} where rmse < mae - 1e-9
