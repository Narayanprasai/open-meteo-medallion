-- int_daily_errors keeps only complete local days, which are 23, 24 or 25 hours long.
select * from {{ ref('int_daily_errors') }} where expected_hours not in (23, 24, 25)
