---
name: weather-pipeline
description: Use when working on this Open-Meteo medallion pipeline: adding a location or weather variable, changing bronze/silver/gold models, running or debugging ingestion, backfills, dbt or Dagster.
---

# Weather pipeline skill

Knowledge transfer guide for the Open-Meteo medallion pipeline. Read this before changing anything.

## Purpose

Forecast weather for Australian locations up to 7 days ahead, measure forecast accuracy against actuals, show how accuracy decays with lead time, and compare two forecast models.

## Stack

| Concern | Tool |
|---|---|
| Ingestion | Python (httpx, tenacity retries), raw JSON to `data/bronze/` |
| Storage and compute | DuckDB, single file `data/warehouse.duckdb` |
| Transformation and tests | dbt (dbt-duckdb) in `dbt/` |
| Orchestration | Dagster in `src/weather_pipeline/orchestration/` |
| Environment | uv, Python 3.12 (pinned in `.python-version`) |

## Data sources

| Purpose | API |
|---|---|
| Live 7 day forecast | Forecast API |
| Forecast at fixed lead days 1 to 7 | Previous Runs API (`<var>_previous_dayN`) |
| Actuals (truth) | Historical Weather API with `models=era5`, lags about 6 days |

Models compared are configured in `config/pipeline.yaml` (ECMWF IFS 0.25° and GFS). Never use "best match": it can change model across lead days and contaminates accuracy curves. `scripts/probe_api.py` verifies any model or variable before it goes into config; rerun it before adding one.

## Non negotiable conventions

1. **UTC in bronze and silver.** Request `timezone=GMT`. Convert to local time only in gold, using the IANA tz from `config/locations.yaml`. Never store a fixed UTC offset.
2. **Local days are 23, 24 or 25 hours** (DST start and end). Daily aggregates are built in dbt from hourly UTC data, never from the API's daily endpoint.
3. **Precipitation is accumulated over the preceding hour.** The value at 10:00 covers 09:00 to 10:00.
4. **Bronze is append only.** One raw file per API request plus a row in `bronze.ingestion_log` (also for failures). A response is skipped as `duplicate` only when the same request (`request_hash`) returned the same data (`payload_hash`, which ignores the volatile `generationtime_ms`).
4a. **Batched responses are lists in request order.** The response has no location id; `location_ids` in the log row gives the order. Column names are `<variable>[_previous_dayN]_<model>`; match the model suffix against the configured list, never split on `_`.
5. **Business dedup happens in silver**, never in bronze. Keep the latest ingestion per grain:
   * `silver_forecast_current`: location, model, valid_time_utc
   * `silver_forecast_by_lead`: location, model, valid_time_utc, lead_day
   * `silver_actuals`: location, valid_time_utc
6. **Truth is ERA5, never a model's own day 0.** Using a model's analysis as truth biases the model comparison.
7. **Paired scoring.** `int_hourly_errors` keeps an hour only if every model and the actual exist, so all models are scored on identical hours (test: `assert_models_scored_on_identical_hours`).
8. **Local time only via macros** in `dbt/macros/local_time.sql` (`to_local`, `local_day_hours`, `au_season`). Never add a fixed offset.
9. **Daily metrics use complete local days only** (23/24/25 hours). A rain day is >= `rain_day_threshold_mm` (1 mm).
10. **Gold tables have a `location_id = 'ALL'` row** (grouping sets) next to per location rows; filter on it for national figures.

## Common tasks

### Add a location
1. Add an entry to `config/locations.yaml` with `id`, `name`, `state`, `latitude`, `longitude`, `timezone` (IANA). Append at the end.
2. Rerun the backfill (see Run). Requests change, so every chunk is stored again with the new location included.
3. Run `dbt build`; uniqueness and hour count tests must pass.

### Answer "how accurate is X?"
Start from `gold.gold_daily_accuracy` (what people read: daily max/min, rain total) or `gold.gold_accuracy_by_lead` (hourly). Filter `location_id = 'ALL'` and `season = 'all'` for headline numbers. Mention the ERA5 caveat whenever comparing models.

### Add a weather variable
1. Add it under `scored_variables` (compared with ERA5 at every lead) or `display_variables` (live forecast only) in `config/pipeline.yaml`. Run `scripts/probe_api.py --only 6` first: a scored variable must be complete for every model, every lead day and in ERA5.
2. Rerun ingestion; bronze keeps the raw payload so nothing else changes there.
3. Add a `max(value) filter (where variable = '...')` column to the relevant silver models and an `accepted_range` test in `dbt/models/silver/silver.yml`. Staging needs no change: it unpacks every field generically.

## Run

Run from the repository root. Short forms: `make setup | test | backfill | run | build | ui | forecast LOC=syd`.

```bash
uv sync                                                       # install
uv run pytest                                                 # tests, no network needed
uv run python -m weather_pipeline.ingest                      # incremental: forecast + last 10 days
uv run python -m weather_pipeline.ingest --source previous_runs actuals \
    --start 2025-10-01 --end 2026-10-14                       # backfill, monthly chunks
uv run dbt build --project-dir dbt --profiles-dir dbt         # staging + silver + data tests
uv run dbt build --project-dir dbt --profiles-dir dbt -s silver_actuals+   # one model and downstream
```

Always run from the repository root: bronze file paths in the log and the DuckDB path are relative to it.

### Orchestration (Dagster)
* `make ui` starts Dagster at http://localhost:3000. Assets are grouped bronze, staging, silver, intermediate, gold; dbt tests are asset checks.
* `bronze/ingestion_log` and `bronze/locations` are produced by the `bronze_ingestion` asset and are the dbt sources, which links the graph. Keep those keys if you rename anything.
* dagster-dbt runs dbt inside `dbt/`, so `definitions.py` passes absolute `WAREHOUSE_PATH` and `bronze_dir`. Never rely on relative paths there.
* Backfill from the UI: materialize `bronze_ingestion` with config `{sources: [previous_runs, actuals], start: "YYYY-MM-DD", end: "YYYY-MM-DD"}`.
* After changing dbt models, restart `dagster dev` (it rebuilds the manifest on start).

Inspect bronze:

```bash
uv run python -c "import duckdb; print(duckdb.connect('data/warehouse.duckdb').sql('select source, status, count(*) from bronze.ingestion_log group by all order by all'))"
```

A failed request is logged with the API's reason and the run exits non zero; other requests continue. Rerunning is always safe and cheap: closed windows already stored after they settled are skipped, so only failed or still changing windows are fetched.

## Gotchas

* Australian zones: WA +8, NT +9:30, SA +9:30/+10:30, QLD +10, NSW/VIC/ACT/TAS +10/+11. QLD, WA, NT have no DST.
* Broken Hill is in NSW but uses SA time: timezone belongs to the location, not the state.
* Returned grid coordinates differ from requested ones; store both.
* Lead days beyond a model's horizon are null, not zero.
* BOM ACCESS Global returns no data for Australian points, live or archived. ICON has no day 7.
* Archive `best_match` fills recent days with forecasts (values appear in the future). Always pin `models=era5`.
* ECMWF has `weather_code` only at day 0, which is why it is a display variable.
* Large requests count as many calls against the free tier; backfills pause between monthly chunks.
* ERA5 is ECMWF's own reanalysis on the ECMWF grid: state this caveat whenever the model comparison is shown.
* ECMWF archive gap 17 to 22 April 2026 (3 columns). Check `silver.silver_coverage` after any backfill.
* dbt tests use local generic tests in `dbt/tests/generic/`; no `dbt deps` or network needed.
* DuckDB memory is capped at 2 GB in `dbt/profiles.yml` (`DUCKDB_MEMORY_LIMIT` to change). Unpack bronze as typed columns plus UNPIVOT, never by extracting JSON per field: that ran out of memory on an 8 GB laptop.
* The API can return HTTP 200 with an empty body on large requests. The client retries it; every stored file is checked for one item per location.
