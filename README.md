# Open-Meteo medallion pipeline

End to end local data pipeline on the [Open-Meteo](https://open-meteo.com/en/docs) API, built with a medallion architecture.

## What it answers

1. What is the forecast for each Australian location for the next 7 days?
2. How accurate were past forecasts compared with actuals?
3. How does accuracy decay as the forecast lead time grows from 1 to 7 days?
4. Which of two forecast models is more accurate?

## Architecture

| Layer | Contents | Tool |
|---|---|---|
| Bronze | Raw API responses, append only, idempotent via payload hash | Python, JSON files, DuckDB |
| Silver | Typed, UTC, deduplicated: current forecast, forecast by lead day, actuals | dbt |
| Gold | 7 day forecast with expected error, accuracy by lead day, model comparison, pipeline health | dbt |

Orchestrated by Dagster. Everything runs locally against a single DuckDB file.

## Key design decisions

* Times stored in UTC; local time derived in gold from IANA timezones (Australia has five offsets and mixed DST).
* Forecast models are pinned, not "best match", so accuracy curves are not affected by model switching.
* Actuals come from ERA5 reanalysis, an independent truth for a fair model comparison.

## Run locally

Requires [uv](https://docs.astral.sh/uv/). Python 3.12 is installed automatically.

```bash
uv sync
uv run pytest                                                  # offline tests
uv run python -m weather_pipeline.ingest --source previous_runs actuals \
    --start 2025-10-01 --end 2026-10-14                        # one off backfill (about 5 to 10 minutes)
uv run python -m weather_pipeline.ingest                       # daily incremental run
uv run dbt build --project-dir dbt --profiles-dir dbt          # silver models and data tests
```

Run every command from the repository root. DuckDB is capped at 2 GB (`DUCKDB_MEMORY_LIMIT=4GB uv run dbt build ...` to raise it). Gold and orchestration steps are added as they land.

## Data model

| Layer | Object | Grain |
|---|---|---|
| Bronze | `data/bronze/<source>/ingest_date=*/*.json`, `bronze.ingestion_log`, `bronze.locations` | One raw file per API request |
| Staging | `staging.stg_bronze__hourly_values` | One value per source, location, model, variable, lead day, hour |
| Silver | `silver.silver_forecast_by_lead` | location, model, valid hour (UTC), lead day 1 to 7 |
| Silver | `silver.silver_actuals` | location, valid hour (UTC), ERA5 |
| Silver | `silver.silver_forecast_current` | location, model, valid hour (UTC), latest live forecast |
| Silver | `silver.silver_coverage`, `silver.dim_location` | completeness report; locations with timezone |

Silver keeps the most recent download per key, and a real value always beats a null one, so late ERA5 values fill in on later runs.

## Known limitations

* Truth is ERA5 reanalysis, not station observations. ERA5 is produced by ECMWF on the same 0.25° grid as `ecmwf_ifs025`, which likely favours ECMWF in the comparison.
* Open-Meteo's ECMWF archive misses three columns for 17 to 22 April 2026 (see `silver.silver_coverage`). Those hours are excluded from scoring for both models.
* Forecasts are for grid points nearest each city, not the exact coordinates.

## Investigation before building

`scripts/probe_api.py` tested every assumption against the live API before any pipeline code was written: which models have forecasts for lead days 1 to 7 over Australia, how far back each archive goes, which variables are complete, which truth source is honest, and whether one full size request succeeds. Findings and decisions are in `docs/ai_log.md`. Rerun it before adding a model or variable.

## Repository layout

```
config/          locations and pipeline settings
src/             ingestion code
dbt/             silver and gold models, tests
orchestration/   Dagster definitions
data/            local bronze files and DuckDB warehouse (gitignored)
docs/            design notes and AI usage evidence
.claude/skills/  knowledge transfer skill for engineers and AI agents
```

## AI usage

AI tools were used throughout. Evidence and decisions are in `docs/ai_log.md`.
