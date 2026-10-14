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
| Environment | uv, Python 3.11+ |

## Data sources

| Purpose | API |
|---|---|
| Live 7 day forecast | Forecast API |
| Forecast at fixed lead days 1 to 7 | Previous Runs API (`<var>_previous_dayN`) |
| Actuals (truth) | Historical Weather API (ERA5), lags a few days |

Models compared are configured in `config/pipeline.yaml`. Never use "best match": it can change model across lead days and contaminates accuracy curves.

## Non negotiable conventions

1. **UTC in bronze and silver.** Request `timezone=GMT`. Convert to local time only in gold, using the IANA tz from `config/locations.yaml`. Never store a fixed UTC offset.
2. **Local days are 23, 24 or 25 hours** (DST start and end). Daily aggregates are built in dbt from hourly UTC data, never from the API's daily endpoint.
3. **Precipitation is accumulated over the preceding hour.** The value at 10:00 covers 09:00 to 10:00.
4. **Bronze is append only.** One file per API request plus a metadata row. `payload_hash` (sha256 of body) makes reruns idempotent: identical payloads are skipped.
5. **Business dedup happens in silver**, never in bronze. Keep the latest ingestion per grain:
   * `silver_forecast_current`: location, model, valid_time_utc
   * `silver_forecast_by_lead`: location, model, valid_time_utc, lead_day
   * `silver_actuals`: location, valid_time_utc
6. **Truth is ERA5, never a model's own day 0.** Using a model's analysis as truth biases the model comparison.
7. **Model comparison uses an inner join** on location, valid_time_utc, lead_day so both models are scored on identical rows.

## Common tasks

### Add a location
1. Add an entry to `config/locations.yaml` with `id`, `name`, `state`, `latitude`, `longitude`, `timezone` (IANA).
2. Run a backfill for that location only (see Run).
3. Run `dbt build`; uniqueness and hour count tests must pass.

### Add a weather variable
1. Add it under `hourly_variables` in `config/pipeline.yaml`. Check it exists in the Previous Runs API if accuracy is needed.
2. Rerun ingestion; bronze keeps the raw payload so nothing else changes there.
3. Add the column to the silver models and an accepted range test.

## Run

Commands are filled in as each step lands. See `README.md`.

## Gotchas

* Australian zones: WA +8, NT +9:30, SA +9:30/+10:30, QLD +10, NSW/VIC/ACT/TAS +10/+11. QLD, WA, NT have no DST.
* Broken Hill is in NSW but uses SA time: timezone belongs to the location, not the state.
* Returned grid coordinates differ from requested ones; store both.
* Lead days beyond a model's horizon are null, not zero.
