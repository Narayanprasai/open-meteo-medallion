-- Local day length must follow each zone's DST rules (checked against known 2025/26 transition dates).
-- Fails if the timezone handling ever regresses (e.g. a fixed UTC offset sneaks in).
with expected as (
    select * from (values
        ('Australia/Sydney',      date '2025-10-05', 23), ('Australia/Sydney',      date '2026-04-05', 25),
        ('Australia/Adelaide',    date '2025-10-05', 23), ('Australia/Adelaide',    date '2026-04-05', 25),
        ('Australia/Broken_Hill', date '2025-10-05', 23), ('Australia/Broken_Hill', date '2026-04-05', 25),
        ('Australia/Brisbane',    date '2025-10-05', 24), ('Australia/Brisbane',    date '2026-04-05', 24),
        ('Australia/Perth',       date '2025-10-05', 24), ('Australia/Darwin',      date '2026-04-05', 24),
        ('Australia/Sydney',      date '2026-01-15', 24)
    ) t(timezone, local_date, hours)
)
select *, {{ local_day_hours('local_date', 'timezone') }} as computed
from expected
where {{ local_day_hours('local_date', 'timezone') }} <> hours
