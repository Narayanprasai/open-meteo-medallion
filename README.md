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

Requires [uv](https://docs.astral.sh/uv/) and `make`. Python 3.12 and every dependency are installed by `make setup`.

```bash
make setup      # install
make test       # offline tests (26)
make backfill   # one off: 12 months of history (5 to 10 minutes), then builds every model and runs 82 data tests
make forecast   # 7 day outlook for Sydney; make forecast LOC=per for Perth (ids in config/locations.yaml)
make run        # daily refresh through Dagster: new forecast + recent actuals, then all models and tests
make ui         # Dagster UI at http://localhost:3000: lineage graph, run history, daily 07:30 schedule
```

Without make, the same steps are:

```bash
uv sync
uv run python -m weather_pipeline.ingest --source previous_runs actuals --start 2025-10-01 --end 2026-10-14
uv run python -m weather_pipeline.ingest                       # daily incremental ingestion
uv run dbt build --project-dir dbt --profiles-dir dbt          # all models and data tests
DAGSTER_HOME=$PWD/.dagster uv run dagster dev                  # orchestration UI
```

Run every command from the repository root. DuckDB is capped at 2 GB (`DUCKDB_MEMORY_LIMIT=4GB` to raise it).

## Orchestration

Dagster (`src/weather_pipeline/orchestration/`) runs the pipeline as one asset graph, grouped by layer:

```
bronze  ->  staging  ->  silver  ->  intermediate  ->  gold
```

* `bronze_ingestion` calls the Python ingester and records stored, duplicate, skipped and failed counts. If any request fails, the asset fails and downstream models are not rebuilt on partial data.
* Every dbt model is an asset and every dbt test is an asset check on it (59 checks).
* `daily_refresh` job: incremental ingestion then `dbt build`. Its schedule (07:30 Australia/Sydney) ships switched off; turn it on in the UI.
* Backfills: launch `bronze_ingestion` from the UI Launchpad with config, e.g. `sources: [previous_runs, actuals]`, `start: "2025-10-01"`.

## Data model

| Layer | Object | Grain |
|---|---|---|
| Bronze | `data/bronze/<source>/ingest_date=*/*.json`, `bronze.ingestion_log`, `bronze.locations` | One raw file per API request |
| Staging | `staging.stg_bronze__hourly_values` | One value per source, location, model, variable, lead day, hour |
| Silver | `silver.silver_forecast_by_lead` | location, model, valid hour (UTC), lead day 1 to 7 |
| Silver | `silver.silver_actuals` | location, valid hour (UTC), ERA5 |
| Silver | `silver.silver_forecast_current` | location, model, valid hour (UTC), latest live forecast |
| Silver | `silver.silver_coverage`, `silver.dim_location`, `silver.weather_codes` | completeness report; locations with timezone; WMO code labels |
| Intermediate | `intermediate.int_hourly_errors` | paired forecast vs actual per hour (all models scored on identical hours) |
| Intermediate | `intermediate.int_daily_errors` | complete local days only (23, 24 or 25 hours with DST) |
| Gold | `gold.gold_forecast_7day` | location, model, local date: outlook with expected error at that lead |
| Gold | `gold.gold_accuracy_by_lead` | model, variable, lead day, location ('ALL' or one): MAE, bias, RMSE, growth vs day 1 |
| Gold | `gold.gold_daily_accuracy` | model, metric (daily max/min temp, rain total, peak wind), lead day, location, season |
| Gold | `gold.gold_rain_skill` | model, lead day, location: rain day hit rate, false alarms, CSI, frequency bias |
| Gold | `gold.gold_model_comparison` | variable, lead day, location: rank, gap to best, share of hours closest |
| Gold | `gold.gold_pipeline_health` | per source: freshness, ERA5 watermark, failures, completeness |

Silver keeps the most recent download per key, and a real value always beats a null one, so late ERA5 values fill in on later runs.

## Results (backfill 1 Oct 2025 to 14 Oct 2026)

| Question | Answer |
|---|---|
| How accurate? | Daily max temperature, all cities: ECMWF typical error 1.2 °C one day ahead (83% of days within 2 °C) |
| Decay with lead time | ECMWF 1.2 °C at day 1 to 2.2 °C at day 7 (58% within 2 °C); GFS 1.4 °C to 2.4 °C |
| Which model? | ECMWF has the lower error at every lead day nationally and is closer in ~60% of hours at day 1 (see ERA5 caveat) |
| Rain | ECMWF catches 86% of rain days at day 1 but slightly over forecasts rain; GFS misses more rain days as lead grows (46% caught at day 7) |
| Local differences | Sydney daily max is forecast ~2 °C too cold and Perth ~2 °C too warm by ECMWF; in Sydney GFS is the better model |

Reproduce:

```bash
uv run python -c "import duckdb; c=duckdb.connect('data/warehouse.duckdb', read_only=True); print(c.sql(\"select lead_day, model, round(mae,2) mae, round(share_within_2,2) within_2c from gold.gold_daily_accuracy where metric='temperature_max' and location_id='ALL' and season='all' order by lead_day, model\"))"
```

## Known limitations

* Truth is ERA5 reanalysis, not station observations. ERA5 is produced by ECMWF on the same 0.25° grid as `ecmwf_ifs025`, which likely favours ECMWF in the comparison.
* Open-Meteo's ECMWF archive misses three columns for 17 to 22 April 2026 (see `silver.silver_coverage`). Those hours are excluded from scoring for both models.
* Forecasts are for grid points nearest each city, not the exact coordinates.
* The large Sydney (cold) and Perth (warm) biases are likely coastal grid cell effects (land and sea mixed differently in forecast and reanalysis). Not yet verified.
* A "lead N" daily value aggregates hourly lead N forecasts, which come from model runs a few hours apart.
* Partial local days at the edges of the data are excluded from daily scores; today and day 7 in the outlook are partial (`is_complete_day`).
* No state level rollup: averaging e.g. Perth with Broome would be meaningless, so results are per location with a `state` column.

## Investigation before building

`scripts/probe_api.py` tested every assumption against the live API before any pipeline code was written: which models have forecasts for lead days 1 to 7 over Australia, how far back each archive goes, which variables are complete, which truth source is honest, and whether one full size request succeeds. Findings and decisions are in `docs/ai_log.md`. Rerun it before adding a model or variable.

## Repository layout

```
config/          locations and pipeline settings
src/             ingestion (weather_pipeline/ingest) and Dagster (weather_pipeline/orchestration)
dbt/             silver and gold models, tests
data/            local bronze files and DuckDB warehouse (gitignored)
docs/            design notes and AI usage evidence
.claude/skills/  knowledge transfer skill for engineers and AI agents
```

## AI usage

AI tools were used throughout. Evidence and decisions are in `docs/ai_log.md`.
