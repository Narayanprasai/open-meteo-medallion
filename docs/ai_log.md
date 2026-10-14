# AI usage log

Material decisions made with AI assistance. Screenshots are in `docs/screenshots/`.

| # | Topic | AI suggestion or finding | Decision |
|---|---|---|---|
| 1 | Stack | DuckDB, dbt, Dagster over Airflow for a local build | Accepted |
| 2 | Accuracy data | Previous Runs API gives lead day 1 to 7 forecasts, so accuracy works from day one | Accepted |
| 3 | Bronze dedup | Use payload hash for idempotency, business dedup in silver, so forecast revisions are kept | Accepted |
| 4 | Truth source | ERA5 reanalysis, not a model's own day 0, to keep the model comparison fair | Accepted |
| 5 | Time handling | UTC until gold; IANA timezones per location for Australian DST edge cases | Accepted |
| 6 | Environment | uv defaulted to Python 3.15 and compiled DuckDB from source | Pinned Python 3.12 via `.python-version` |
| 7 | Verify before building | AI could not reach the API, so wrote `scripts/probe_api.py` to test assumptions; I ran it locally | Accepted |
| 8 | Model choice | Probe: BOM ACCESS Global returns no data live or archived; ICON lacks day 7; ECMWF 9 km archive starts 2026 | ECMWF IFS 0.25° vs NOAA GFS |
| 9 | Truth leakage | Probe: archive `best_match` returned values 12 hours in the future (forecast filled) | Pin `models=era5`; accuracy watermark is ERA5 lag (~6 days) |
| 10 | Backfill window | Both models archived from at least July 2024 | 12 months from 2025-10-01, configurable |
| 11 | Variable coverage | Probe: all scored variables complete for both models at every lead and in ERA5; ECMWF weather_code only at day 0 | Split config into scored and display variables |
| 12 | Request size | Probe: one month, 12 locations, 2 models, 80 columns returns in ~10 s, ~3 MB | Backfill in monthly chunks with a pause between requests |
