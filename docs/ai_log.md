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
| 13 | Bronze dedup bug | A test showed two different months with identical data were marked duplicate, pointing at the wrong file | Dedupe on request hash plus payload hash; regression test added |
| 14 | Volatile field | Every response has `generationtime_ms`, which changes per call, so a raw byte hash never matches | Payload hash ignores volatile fields |
| 15 | Real backfill failure | API returned HTTP 200 with an empty body for August 2026; the run crashed | Treat a non JSON 200 as transient and retry; validate one item per location; any unexpected error is logged per request and the run continues |
| 16 | Cheap reruns | After a failure, rerunning refetched every month | Skip closed windows already stored after they settled; windows stored while ERA5 was still filling in are fetched again |
| 17 | Silver unpacking | Single model requests (ERA5) have no model suffix in field names; batched responses carry no location id | Take the model from the ingestion log when unsuffixed; map list position to `location_ids` from the log |
| 18 | Archive gap | Silver coverage found ECMWF missing 3 columns (RH lead 1, precip leads 5 and 7) for 17 to 22 April 2026, all cities | Keep as null, report in `silver_coverage`, score only hours where forecast and actual both exist |
| 19 | Truth caveat | ERA5 is ECMWF's own reanalysis on the same 0.25° grid as `ecmwf_ifs025`, a likely home advantage | Keep ERA5 (best independent option available) and state the caveat with the comparison results |
| 20 | No dbt packages | `dbt_utils` would need network access at build time | Small local generic tests: `unique_combination`, `accepted_range`, `has_all_locations` |
| 21 | Out of memory on 8 GB Mac | Staging copied each city's raw JSON once per field (and a 256 MB read buffer per thread); peaked above 6 GB. Not seen in the AI workspace, which had more memory | Read files as typed columns and UNPIVOT; read buffer 64 MB; memory capped at 2 GB in the dbt profile. Verified the full build passes at 1 GB |
| 22 | Silent empty staging | Matching files to log rows on full path returned zero rows when files lived elsewhere | Match on `<source>/ingest_date=*/<name>.json`; new test fails if any stored file produced no rows |
| 23 | DST verification | Checked day lengths before building gold: 23 h and 25 h days in DST zones, always 24 h in QLD, WA, NT, correct :30 offsets and Broken Hill on SA time | Local time derived only in gold via IANA zones; regression test on known transition dates |
| 24 | Fair comparison | Scoring each model on its own available hours would let archive gaps skew the result | Paired scoring: an hour counts only if every model and the actual exist; test asserts identical hour counts |
| 25 | Daily metrics | Partial local days would bias daily max/min | Keep only complete local days (23/24/25 h); rain days use the BOM 1 mm convention |
| 26 | Unexpected finding | Sydney forecast ~2 °C too cold and Perth ~2 °C too warm, unlike other cities | Reported as a finding with a hypothesis (coastal grid cells), not explained away |
| 27 | Failed downloads | A failed request is an operational issue, not bad data | Health check is a dbt warning, not a build failure |
