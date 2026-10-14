"""Dagster definitions: graph shape, schedule, and bronze asset behaviour (ingestion faked, no network)."""

import os

os.environ.setdefault("DAGSTER_IS_DEV_CLI", "1")  # lets dagster-dbt build the dbt manifest on import

import pytest
from dagster import AssetKey, DagsterInstance, materialize

from weather_pipeline.ingest.bronze import LogEntry
from weather_pipeline.ingest.run import RunSummary
from weather_pipeline.orchestration import definitions as d


def test_graph_links_bronze_to_dbt_and_groups_by_layer():
    graph = d.defs.resolve_asset_graph()
    staging = AssetKey(["staging", "stg_bronze__hourly_values"])
    assert AssetKey(["bronze", "ingestion_log"]) in graph.get(staging).parent_keys

    groups = {graph.get(k).group_name for k in graph.get_all_asset_keys()}
    assert groups == {"bronze", "staging", "silver", "intermediate", "gold"}
    assert len(list(graph.asset_check_keys)) > 50  # dbt tests surface as asset checks


def test_daily_schedule_runs_in_sydney_time():
    schedule = d.defs.resolve_schedule_def("daily_refresh_schedule")
    assert schedule.cron_schedule == "30 7 * * *"
    assert schedule.execution_timezone == "Australia/Sydney"
    assert schedule.job_name == "daily_refresh"


def _fake_run(status: str):
    def fake(settings, sources, start=None, end=None):
        entry = LogEntry("req-1", status, "hash", "data/bronze/x.json", "boom" if status == "failed" else None)
        return RunSummary("run-1", [entry], skipped=2)
    return fake


def _materialize_bronze(run_config=None):
    return materialize([d.bronze_ingestion], instance=DagsterInstance.ephemeral(),
                       run_config=run_config, raise_on_error=False)


def test_bronze_asset_reports_counts(monkeypatch):
    monkeypatch.setattr(d, "run_ingestion", _fake_run("stored"))
    result = _materialize_bronze()
    assert result.success
    [log_mat] = [m for m in result.asset_materializations_for_node("bronze_ingestion")
                 if m.asset_key == AssetKey(["bronze", "ingestion_log"])]
    assert log_mat.metadata["stored"].value == 1
    assert log_mat.metadata["skipped"].value == 2


def test_bronze_asset_fails_when_a_request_fails(monkeypatch):
    monkeypatch.setattr(d, "run_ingestion", _fake_run("failed"))
    assert not _materialize_bronze().success


def test_bronze_asset_passes_backfill_config(monkeypatch):
    seen = {}

    def fake(settings, sources, start=None, end=None):
        seen.update(sources=sources, start=start, end=end)
        return RunSummary("run-1", [], skipped=0)

    monkeypatch.setattr(d, "run_ingestion", fake)
    config = {"ops": {"bronze_ingestion": {"config": {
        "sources": ["previous_runs", "actuals"], "start": "2025-10-01", "end": "2026-10-14"}}}}
    assert _materialize_bronze(config).success
    assert seen["sources"] == ["previous_runs", "actuals"]
    assert str(seen["start"]) == "2025-10-01" and str(seen["end"]) == "2026-10-14"


def test_unknown_source_is_rejected(monkeypatch):
    monkeypatch.setattr(d, "run_ingestion", _fake_run("stored"))
    config = {"ops": {"bronze_ingestion": {"config": {"sources": ["satellite"]}}}}
    assert not _materialize_bronze(config).success
