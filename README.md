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

Instructions added as each step lands.

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
