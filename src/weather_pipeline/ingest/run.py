"""Run bronze ingestion for one or more sources.

Windows (UTC dates)
* forecast:       always the live next forecast_days; no window.
* previous_runs:  backfill uses --start/--end; incremental uses the last actuals_lookback_days.
* actuals:        same windows. Recent days are re-fetched because ERA5 fills in with a ~6 day lag.

Closed windows (ending before the lookback period) are skipped when the same request was stored after
the window closed: that data no longer changes, so a rerun after a failure only fetches what is missing.
A copy stored while the window was still open (ERA5 not yet filled in) is fetched again.

One failed request never stops the others, whatever the error; the run exits non zero if any failed.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from weather_pipeline.config import Settings
from weather_pipeline.ingest.bronze import BronzeStore, LogEntry
from weather_pipeline.ingest.client import ApiError, OpenMeteoClient, TransientApiError
from weather_pipeline.ingest.requests import SOURCES, build_requests

log = logging.getLogger("weather_pipeline.ingest")


@dataclass
class RunSummary:
    run_id: str
    entries: list[LogEntry]
    skipped: int = 0

    def count(self, status: str) -> int:
        return sum(e.status == status for e in self.entries)

    @property
    def ok(self) -> bool:
        return self.count("failed") == 0


def utc_today() -> date:
    return datetime.now(timezone.utc).date()


def default_window(settings: Settings, today: date | None = None) -> tuple[date, date]:
    today = today or utc_today()
    return today - timedelta(days=settings.actuals_lookback_days), today


def run_ingestion(settings: Settings, sources: list[str], start: date | None = None, end: date | None = None,
                  client: OpenMeteoClient | None = None, store: BronzeStore | None = None) -> RunSummary:
    default_start, default_end = default_window(settings)
    start = max(start or default_start, settings.backfill_start)
    end = end or default_end
    run_id = str(uuid.uuid4())
    client = client or OpenMeteoClient()
    store = store or BronzeStore(settings.root, settings.bronze_dir, settings.warehouse_path)
    entries: list[LogEntry] = []
    store.sync_locations(settings.locations)
    closed_before = utc_today() - timedelta(days=settings.actuals_lookback_days)

    specs = [spec for source in sources for spec in build_requests(settings, source, start, end)]
    windowed = [s for s in sources if s != "forecast"]
    window = f", window {start} to {end}" if windowed else ""
    log.info("run %s: %d requests for %s%s", run_id[:8], len(specs), ",".join(sources), window)

    skipped = 0
    fetched = 0
    try:
        for spec in specs:
            label = f"{spec.source} {spec.window_start or 'live'}..{spec.window_end or ''}"
            settled_on = spec.window_end + timedelta(days=settings.actuals_lookback_days) if spec.window_end else None
            if settled_on and spec.window_end < closed_before and store.is_stored_after(spec, settled_on):
                log.info("%-40s skipped (closed window, already stored)", label)
                skipped += 1
                continue
            if fetched and settings.pause_between_requests_seconds:
                time.sleep(settings.pause_between_requests_seconds)
            fetched += 1
            try:
                response = client.get(spec.url, spec.params)
                entry = store.record_success(run_id, spec, response.status, response.content)
                log.info("%-40s %s %s", label, entry.status, entry.file_path)
            except ApiError as exc:
                entry = store.record_failure(run_id, spec, exc.status, exc.reason)
                log.error("%-40s failed: %s", label, exc)
            except (TransientApiError, OSError) as exc:  # retries exhausted, or network gone
                entry = store.record_failure(run_id, spec, None, str(exc))
                log.error("%-40s failed after retries: %s", label, exc)
            except Exception as exc:  # anything unexpected: log it, keep going
                entry = store.record_failure(run_id, spec, None, f"{type(exc).__name__}: {exc}")
                log.exception("%-40s failed unexpectedly", label)
            entries.append(entry)
    finally:
        client.close()
        store.close()

    summary = RunSummary(run_id, entries, skipped)
    log.info("run %s done: %d stored, %d duplicate, %d skipped, %d failed", run_id[:8],
             summary.count("stored"), summary.count("duplicate"), skipped, summary.count("failed"))
    return summary


__all__ = ["SOURCES", "RunSummary", "run_ingestion", "default_window"]
