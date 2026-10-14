# One word commands. Run from the repository root. Requires uv (https://docs.astral.sh/uv/).
export DAGSTER_HOME := $(CURDIR)/.dagster
BACKFILL_START ?= 2025-10-01
BACKFILL_END   ?= $(shell date -u +%Y-%m-%d)
DBT := uv run dbt --no-use-colors
DBT_ARGS := --project-dir dbt --profiles-dir dbt

.PHONY: setup test backfill run build ui forecast clean

setup:      ## install Python 3.12 and all dependencies
	uv sync
	mkdir -p $(DAGSTER_HOME)

test:       ## offline Python tests
	uv run pytest -q

backfill:   ## one off: 12 months of past forecasts and ERA5 actuals (5 to 10 minutes), then build all models
	uv run python -m weather_pipeline.ingest --source previous_runs actuals --start $(BACKFILL_START) --end $(BACKFILL_END)
	$(MAKE) run

run: setup  ## daily refresh through Dagster: incremental ingestion, then every dbt model and test
	uv run dagster job execute -m weather_pipeline.orchestration -j daily_refresh

build:      ## rebuild and test dbt models only (no API calls)
	$(DBT) build $(DBT_ARGS)

ui: setup   ## Dagster UI at http://localhost:3000 (lineage graph, runs, schedule)
	uv run dagster dev

forecast:   ## print the 7 day outlook for one location, e.g. make forecast LOC=per
	uv run python -c "import duckdb; print(duckdb.connect('data/warehouse.duckdb', read_only=True).sql(\"select model, local_date, lead_day, temperature_min tmin, temperature_max tmax, expected_tmax_error err, precipitation_sum rain_mm, weather_description from gold.gold_forecast_7day where location_id='$(or $(LOC),syd)' order by local_date, model\"))"

clean:      ## remove build output (keeps downloaded data)
	rm -rf dbt/target dbt/logs logs .pytest_cache
