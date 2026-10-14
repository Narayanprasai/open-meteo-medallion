# AI usage log

Material decisions made with AI assistance. Screenshots are in `docs/screenshots/`.

| # | Topic | AI suggestion | Decision |
|---|---|---|---|
| 1 | Stack | DuckDB, dbt, Dagster over Airflow for a local build | Accepted |
| 2 | Accuracy data | Previous Runs API gives lead day 1 to 7 forecasts back to 2024, so accuracy works from day one | Accepted |
| 3 | Bronze dedup | Use payload hash for idempotency, business dedup in silver, so forecast revisions are kept | Accepted |
| 4 | Truth source | ERA5 reanalysis, not a model's own day 0, to keep the model comparison fair | Accepted |
| 5 | Time handling | UTC until gold; IANA timezones per location for Australian DST edge cases | Accepted |
