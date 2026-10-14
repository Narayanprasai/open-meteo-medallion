"""Dagster definitions: bronze ingestion plus every dbt model and test, as one asset graph.

Lineage shown in the UI:
    bronze/ingestion_log, bronze/locations  (Python ingester)
        -> staging -> silver -> intermediate -> gold  (dbt models)
    dbt tests appear as asset checks on the model they test.

Paths: everything is resolved from the repository root (WEATHER_PIPELINE_ROOT, default: the current
directory). dbt runs inside dbt/, so the warehouse path and bronze directory are passed as absolute
paths; that is why dbt behaves the same here as when run by hand from the root.
"""

# No `from __future__ import annotations` here: Dagster inspects the real type hints at decoration time.
import json
import os
from datetime import date
from pathlib import Path
from typing import Optional

from dagster import (
    AssetExecutionContext,
    AssetSelection,
    AssetSpec,
    Config,
    DefaultScheduleStatus,
    Definitions,
    Failure,
    MaterializeResult,
    ScheduleDefinition,
    define_asset_job,
    multi_asset,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from weather_pipeline.config import load_settings
from weather_pipeline.ingest.requests import SOURCES
from weather_pipeline.ingest.run import run_ingestion

ROOT = Path(os.environ.get("WEATHER_PIPELINE_ROOT", Path.cwd())).resolve()
SETTINGS = load_settings(ROOT)

# dbt resolves these relative to its own working directory, so make them absolute.
os.environ.setdefault("WAREHOUSE_PATH", str(SETTINGS.warehouse_path))
DBT_VARS = {"bronze_dir": str(SETTINGS.bronze_dir)}

dbt_project = DbtProject(project_dir=ROOT / "dbt", profiles_dir=ROOT / "dbt")
dbt_project.prepare_if_dev()  # rebuilds the manifest on every `dagster dev` start
if not dbt_project.manifest_path.exists():  # fresh clone run outside `dagster dev` (e.g. make run)
    dbt_project.preparer.prepare(dbt_project)


# ---------------------------------------------------------------- bronze

class IngestConfig(Config):
    """Run config for the bronze asset. Defaults are the daily incremental run.

    Backfill example (Launchpad):  sources: [previous_runs, actuals], start: "2025-10-01", end: "2026-10-14"
    """

    sources: list[str] = list(SOURCES)
    start: Optional[str] = None
    end: Optional[str] = None


@multi_asset(
    specs=[
        # Keys match the dbt sources (bronze.ingestion_log, bronze.locations), which links the graph.
        AssetSpec(["bronze", "ingestion_log"], group_name="bronze",
                  description="One row per API request; raw responses in data/bronze/<source>/."),
        AssetSpec(["bronze", "locations"], group_name="bronze",
                  description="Mirror of config/locations.yaml."),
    ],
    can_subset=False,
)
def bronze_ingestion(context: AssetExecutionContext, config: IngestConfig):
    """Download forecast, previous runs and ERA5 actuals into bronze (idempotent, safe to rerun)."""
    unknown = set(config.sources) - set(SOURCES)
    if unknown:
        raise Failure(f"Unknown sources {sorted(unknown)}; expected any of {list(SOURCES)}")

    summary = run_ingestion(
        SETTINGS,
        config.sources,
        start=date.fromisoformat(config.start) if config.start else None,
        end=date.fromisoformat(config.end) if config.end else None,
    )
    counts = {s: summary.count(s) for s in ("stored", "duplicate", "failed")} | {"skipped": summary.skipped}
    context.log.info(f"bronze run {summary.run_id[:8]}: {counts}")

    if not summary.ok:
        failed = [e.error for e in summary.entries if e.status == "failed"]
        # Stop here so downstream models are not rebuilt on a partial download; the log row has details.
        raise Failure(description=f"{counts['failed']} request(s) failed", metadata={"errors": json.dumps(failed)})

    metadata = {"run_id": summary.run_id, **counts, "sources": ", ".join(config.sources)}
    yield MaterializeResult(asset_key=["bronze", "ingestion_log"], metadata=metadata)
    yield MaterializeResult(asset_key=["bronze", "locations"], metadata={"locations": len(SETTINGS.locations)})


# ---------------------------------------------------------------- dbt

class LayerTranslator(DagsterDbtTranslator):
    """Group dbt assets by their schema (staging, silver, intermediate, gold) so the UI shows the layers."""

    def get_group_name(self, dbt_resource_props):
        return dbt_resource_props.get("schema") or "dbt"


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=LayerTranslator())
def weather_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """Staging, silver, intermediate and gold models; dbt tests run as asset checks."""
    yield from dbt.cli(["build", "--vars", json.dumps(DBT_VARS)], context=context).stream()


# ---------------------------------------------------------------- jobs and schedules

daily_refresh = define_asset_job(
    name="daily_refresh",
    description="Incremental ingestion (live forecast + last 10 days) then rebuild and test every model.",
    selection=AssetSelection.all(),
)

# 07:30 Sydney time: overnight model runs are published and ERA5 has advanced by a day.
daily_schedule = ScheduleDefinition(
    name="daily_refresh_schedule",
    job=daily_refresh,
    cron_schedule="30 7 * * *",
    execution_timezone="Australia/Sydney",
    default_status=DefaultScheduleStatus.STOPPED,  # turn on in the UI; a laptop is not always awake
)

defs = Definitions(
    assets=[bronze_ingestion, weather_dbt_models],
    jobs=[daily_refresh],
    schedules=[daily_schedule],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
