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
| Orchestration | Dagster in `orchestration/` |
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
7. **Model comparison uses an inner join** on location, valid_time_utc, lead_day so both models are scored on identical rows.

## Common tasks

### Add a location
1. Add an entry to `config/locations.yaml` with `id`, `name`, `state`, `latitude`, `longitude`, `timezone` (IANA). Append at the end.
2. Rerun the backfill (see Run). Requests change, so every chunk is stored again with the new location included.
3. Run `dbt build`; uniqueness and hour count tests must pass.

### Add a weather variable
1. Add it under `scored_variables` (compared with ERA5 at every lead) or `display_variables` (live forecast only) in `config/pipeline.yaml`. Run `scripts/probe_api.py --only 6` first: a scored variable must be complete for every model, every lead day and in ERA5.
2. Rerun ingestion; bronze keeps the raw payload so nothing else changes there.
3. Add the column to the silver models and an accepted range test.

## Run

Run from the repository root.

```bash
uv sync                                                       # install
uv run pytest                                                 # tests, no network needed
uv run python -m weather_pipeline.ingest                      # incremental: forecast + last 10 days
uv run python -m weather_pipeline.ingest --source previous_runs actuals \
    --start 2025-10-01 --end 2026-10-14                       # backfill, monthly chunks
```

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
* The API can return HTTP 200 with an empty body on large requests. The client retries it; every stored file is checked for one item per location.
