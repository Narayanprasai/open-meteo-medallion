"""Bronze ingestion tests against a mocked Open-Meteo API. No network needed."""

from __future__ import annotations

import dataclasses
import json
import shutil
from datetime import date
from pathlib import Path

import duckdb
import httpx
import pytest

from weather_pipeline.config import load_settings
from weather_pipeline.ingest.bronze import BronzeStore, payload_hash
from weather_pipeline.ingest.client import OpenMeteoClient
from weather_pipeline.ingest.requests import build_requests, month_chunks
from weather_pipeline.ingest.run import run_ingestion

REPO = Path(__file__).resolve().parents[1]


@pytest.fixture
def settings(tmp_path):
    shutil.copytree(REPO / "config", tmp_path / "config")
    return dataclasses.replace(load_settings(tmp_path), pause_between_requests_seconds=0)


def fake_body(request: httpx.Request, generation_ms: float = 0.5) -> list[dict]:
    """One item per requested location, like the real API."""
    params = request.url.params
    n_locations = len(params["latitude"].split(","))
    columns = [f"{v}_{m}" for v in params["hourly"].split(",") for m in params["models"].split(",")]
    return [
        {"latitude": -33.8, "longitude": 151.1, "generationtime_ms": generation_ms, "timezone": "GMT",
         "hourly": {"time": ["2026-09-01T00:00"], **{c: [20.0] for c in columns}}}
        for _ in range(n_locations)
    ]


def make_client(handler) -> OpenMeteoClient:
    return OpenMeteoClient(httpx.Client(transport=httpx.MockTransport(handler)), max_attempts=3, max_wait_seconds=0)


def log_rows(settings) -> list[tuple]:
    with duckdb.connect(str(settings.warehouse_path)) as con:
        return con.execute("select source, status, http_status, error from bronze.ingestion_log order by ingested_at_utc").fetchall()


# ---------- request building ----------

def test_month_chunks_cover_window_without_gaps():
    chunks = month_chunks(date(2025, 10, 15), date(2026, 1, 10))
    assert chunks == [
        (date(2025, 10, 15), date(2025, 10, 31)),
        (date(2025, 11, 1), date(2025, 11, 30)),
        (date(2025, 12, 1), date(2025, 12, 31)),
        (date(2026, 1, 1), date(2026, 1, 10)),
    ]


def test_full_backfill_is_thirteen_monthly_requests(settings):
    assert len(build_requests(settings, "previous_runs", date(2025, 10, 1), date(2026, 10, 14))) == 13


def test_previous_runs_requests_scored_variables_at_every_lead_only(settings):
    spec = build_requests(settings, "previous_runs", date(2026, 9, 1), date(2026, 9, 30))[0]
    hourly = spec.params["hourly"].split(",")
    assert len(hourly) == len(settings.scored_variables) * len(settings.lead_days)
    assert "temperature_2m_previous_day7" in hourly
    assert not any(h.startswith("weather_code") for h in hourly)
    assert spec.params["models"] == "ecmwf_ifs025,gfs_global"
    assert spec.params["timezone"] == "GMT"


def test_forecast_includes_display_variables(settings):
    spec = build_requests(settings, "forecast", date(2026, 9, 1), date(2026, 9, 1))[0]
    assert "weather_code" in spec.params["hourly"].split(",")
    assert spec.params["forecast_days"] == 7


def test_actuals_pin_era5(settings):
    spec = build_requests(settings, "actuals", date(2026, 9, 1), date(2026, 9, 30))[0]
    assert spec.params["models"] == "era5"


def test_location_order_is_recorded(settings):
    spec = build_requests(settings, "forecast", date(2026, 9, 1), date(2026, 9, 1))[0]
    assert spec.location_ids == [loc.id for loc in settings.locations]
    assert len(spec.params["latitude"].split(",")) == len(spec.location_ids)


# ---------- hashing ----------

def test_hash_ignores_generation_time_but_not_data():
    a = json.dumps([{"generationtime_ms": 0.1, "hourly": {"t": [1.0]}}]).encode()
    b = json.dumps([{"generationtime_ms": 9.9, "hourly": {"t": [1.0]}}]).encode()
    c = json.dumps([{"generationtime_ms": 0.1, "hourly": {"t": [2.0]}}]).encode()
    assert payload_hash(a) == payload_hash(b)
    assert payload_hash(a) != payload_hash(c)


# ---------- end to end ----------

def test_rerun_is_idempotent(settings):
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        return httpx.Response(200, json=fake_body(request, generation_ms=calls["n"]))

    for _ in range(2):
        store = BronzeStore(settings.root, settings.bronze_dir, settings.warehouse_path)
        summary = run_ingestion(settings, ["forecast"], client=make_client(handler), store=store)
        assert summary.ok

    files = list(settings.bronze_dir.rglob("*.json"))
    assert len(files) == 1
    assert [r[1] for r in log_rows(settings)] == ["stored", "duplicate"]


def test_raw_body_is_stored_unchanged(settings):
    raw = {}

    def handler(request):
        body = json.dumps(fake_body(request)).encode()
        raw["body"] = body
        return httpx.Response(200, content=body)

    run_ingestion(settings, ["forecast"], client=make_client(handler),
                  store=BronzeStore(settings.root, settings.bronze_dir, settings.warehouse_path))
    [file] = list(settings.bronze_dir.rglob("*.json"))
    assert file.read_bytes() == raw["body"]
    assert "ingest_date=" in str(file)


def test_transient_error_is_retried(settings):
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(503, text="busy")
        return httpx.Response(200, json=fake_body(request))

    summary = run_ingestion(settings, ["forecast"], client=make_client(handler),
                            store=BronzeStore(settings.root, settings.bronze_dir, settings.warehouse_path))
    assert summary.ok
    assert calls["n"] == 2


def test_bad_request_is_not_retried_and_is_logged(settings):
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        return httpx.Response(400, json={"error": True, "reason": "Cannot initialize model from invalid String value"})

    summary = run_ingestion(settings, ["forecast"], client=make_client(handler),
                            store=BronzeStore(settings.root, settings.bronze_dir, settings.warehouse_path))
    assert not summary.ok
    assert calls["n"] == 1
    [(source, status, http_status, error)] = log_rows(settings)
    assert (source, status, http_status) == ("forecast", "failed", 400)
    assert "invalid String" in error


def test_one_failed_chunk_does_not_stop_the_others(settings):
    def handler(request):
        if request.url.params["start_date"] == "2026-08-01":
            return httpx.Response(400, json={"error": True, "reason": "bad"})
        return httpx.Response(200, json=fake_body(request))

    summary = run_ingestion(settings, ["actuals"], date(2026, 7, 1), date(2026, 9, 30), client=make_client(handler),
                            store=BronzeStore(settings.root, settings.bronze_dir, settings.warehouse_path))
    assert [e.status for e in summary.entries] == ["stored", "failed", "stored"]
    assert not summary.ok


def test_start_is_clamped_to_backfill_start(settings):
    seen = []

    def handler(request):
        seen.append(request.url.params["start_date"])
        return httpx.Response(200, json=fake_body(request))

    run_ingestion(settings, ["actuals"], date(2020, 1, 1), date(2025, 10, 5), client=make_client(handler),
                  store=BronzeStore(settings.root, settings.bronze_dir, settings.warehouse_path))
    assert seen == [settings.backfill_start.isoformat()]


def test_different_requests_with_identical_data_are_both_stored(settings):
    def handler(request):
        return httpx.Response(200, json=fake_body(request))  # same body for every month

    summary = run_ingestion(settings, ["actuals"], date(2026, 8, 1), date(2026, 9, 30), client=make_client(handler),
                            store=BronzeStore(settings.root, settings.bronze_dir, settings.warehouse_path))
    assert [e.status for e in summary.entries] == ["stored", "stored"]
    assert len(list(settings.bronze_dir.rglob("*.json"))) == 2


# ---------- resilience (found in the first real backfill) ----------

def test_empty_200_body_is_retried(settings):
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(200, content=b"")
        return httpx.Response(200, json=fake_body(request))

    summary = run_ingestion(settings, ["forecast"], client=make_client(handler),
                            store=BronzeStore(settings.root, settings.bronze_dir, settings.warehouse_path))
    assert summary.ok
    assert calls["n"] == 2


def test_persistent_bad_body_is_logged_and_run_continues(settings):
    def handler(request):
        if request.url.params["start_date"] == "2026-08-01":
            return httpx.Response(200, content=b"")
        return httpx.Response(200, json=fake_body(request))

    summary = run_ingestion(settings, ["actuals"], date(2026, 7, 1), date(2026, 9, 30), client=make_client(handler),
                            store=BronzeStore(settings.root, settings.bronze_dir, settings.warehouse_path))
    assert [e.status for e in summary.entries] == ["stored", "failed", "stored"]
    assert "not valid JSON" in summary.entries[1].error


def test_wrong_location_count_is_rejected(settings):
    def handler(request):
        return httpx.Response(200, json=fake_body(request)[:3])

    summary = run_ingestion(settings, ["forecast"], client=make_client(handler),
                            store=BronzeStore(settings.root, settings.bronze_dir, settings.warehouse_path))
    assert summary.entries[0].status == "failed"
    assert "expected 12 locations, got 3" in summary.entries[0].error
    assert not list(settings.bronze_dir.rglob("*.json"))


def test_closed_window_stored_after_close_is_skipped(settings):
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        return httpx.Response(200, json=fake_body(request))

    def run():
        return run_ingestion(settings, ["actuals"], date(2025, 10, 1), date(2025, 11, 30), client=make_client(handler),
                             store=BronzeStore(settings.root, settings.bronze_dir, settings.warehouse_path))

    run()
    second = run()
    assert calls["n"] == 2          # only the first run fetched
    assert second.skipped == 2


def test_window_stored_while_open_is_fetched_again(settings, monkeypatch):
    import weather_pipeline.ingest.run as run_module
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        return httpx.Response(200, json=fake_body(request))

    def run():
        return run_ingestion(settings, ["actuals"], date(2026, 9, 1), date(2026, 9, 30), client=make_client(handler),
                             store=BronzeStore(settings.root, settings.bronze_dir, settings.warehouse_path))

    monkeypatch.setattr(run_module, "utc_today", lambda: date(2026, 10, 5))   # September still settling
    run()
    with duckdb.connect(str(settings.warehouse_path)) as con:      # pretend it was stored on 5 October
        con.execute("update bronze.ingestion_log set ingested_at_utc = timestamp '2026-10-05 00:00:00'")
    monkeypatch.setattr(run_module, "utc_today", lambda: date(2026, 11, 20))  # long closed now
    second = run()
    assert second.skipped == 0 and calls["n"] == 2


def test_locations_are_synced_from_config(settings):
    run_ingestion(settings, ["forecast"], client=make_client(lambda r: httpx.Response(200, json=fake_body(r))),
                  store=BronzeStore(settings.root, settings.bronze_dir, settings.warehouse_path))
    with duckdb.connect(str(settings.warehouse_path)) as con:
        rows = con.execute("select location_id, timezone from bronze.locations order by sort_order").fetchall()
    assert rows[0] == ("syd", "Australia/Sydney")
    assert ("bhq", "Australia/Broken_Hill") in rows
    assert len(rows) == len(settings.locations)
