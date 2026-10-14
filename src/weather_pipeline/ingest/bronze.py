"""Bronze layer: raw API responses on disk plus an ingestion log in DuckDB.

Rules
* The file holds the response body exactly as received. Bronze never transforms data.
* payload_hash is sha256 of the body with volatile fields removed (generationtime_ms changes on
  every call). request_hash is sha256 of source, url and parameters.
* A response is a duplicate only when the same request returned the same payload before. It is logged
  as 'duplicate' and not written again, so reruns are idempotent. Two different requests that happen
  to return identical data (e.g. all null days) are both stored.
* Every request is logged, including failures, so the log is a complete audit trail.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

from weather_pipeline.ingest.requests import RequestSpec

VOLATILE_FIELDS = {"generationtime_ms"}

LOG_DDL = """
create schema if not exists bronze;
create table if not exists bronze.ingestion_log (
    request_id      varchar primary key,
    run_id          varchar not null,
    source          varchar not null,
    url             varchar not null,
    request_params  json not null,
    location_ids    varchar[] not null,
    models          varchar[] not null,
    window_start    date,
    window_end      date,
    status          varchar not null,      -- stored | duplicate | failed
    request_hash    varchar not null,
    http_status     integer,
    payload_hash    varchar,
    file_path       varchar,               -- relative to the repo root
    bytes           bigint,
    error           varchar,
    ingested_at_utc timestamp not null
);
"""


def payload_hash(content: bytes) -> str:
    """Hash of the body ignoring volatile fields, so identical data hashes identically."""
    body = json.loads(content)

    def strip(obj):
        if isinstance(obj, dict):
            return {k: strip(v) for k, v in obj.items() if k not in VOLATILE_FIELDS}
        if isinstance(obj, list):
            return [strip(v) for v in obj]
        return obj

    canonical = json.dumps(strip(body), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def request_hash(spec: RequestSpec) -> str:
    key = json.dumps({"source": spec.source, "url": spec.url, "params": spec.params}, sort_keys=True, default=str)
    return hashlib.sha256(key.encode()).hexdigest()


class InvalidPayloadError(ValueError):
    """The body parsed but does not have the expected shape."""


def check_shape(content: bytes, spec: RequestSpec) -> None:
    body = json.loads(content)
    items = body if isinstance(body, list) else [body]
    if len(items) != len(spec.location_ids):
        raise InvalidPayloadError(f"expected {len(spec.location_ids)} locations, got {len(items)}")
    missing = [i for i, item in enumerate(items) if "hourly" not in item]
    if missing:
        raise InvalidPayloadError(f"items without 'hourly': {missing}")


@dataclass(frozen=True)
class LogEntry:
    request_id: str
    status: str
    payload_hash: str | None
    file_path: str | None
    error: str | None


class BronzeStore:
    def __init__(self, root: Path, bronze_dir: Path, warehouse_path: Path):
        self.root = root
        self.bronze_dir = bronze_dir
        warehouse_path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(warehouse_path))
        self.con.execute(LOG_DDL)

    def _existing_file(self, req_hash: str, digest: str) -> str | None:
        row = self.con.execute(
            "select file_path from bronze.ingestion_log "
            "where request_hash = ? and payload_hash = ? and status = 'stored' limit 1",
            [req_hash, digest],
        ).fetchone()
        return row[0] if row else None

    def _insert(self, run_id: str, spec: RequestSpec, *, status: str, http_status: int | None,
                digest: str | None, file_path: str | None, size: int | None, error: str | None,
                now: datetime) -> LogEntry:
        request_id = str(uuid.uuid4())
        self.con.execute(
            "insert into bronze.ingestion_log values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [request_id, run_id, spec.source, spec.url, json.dumps(spec.params, sort_keys=True),
             spec.location_ids, spec.models, spec.window_start, spec.window_end, status, request_hash(spec),
             http_status, digest, file_path, size, error, now.replace(tzinfo=None)],
        )
        return LogEntry(request_id, status, digest, file_path, error)

    def is_stored_after(self, spec: RequestSpec, stored_on_or_after: date) -> bool:
        """True when this exact request was stored on or after the given date.

        Used to skip closed windows only if the stored copy was fetched once the data had settled.
        """
        return self.con.execute(
            "select count(*) from bronze.ingestion_log "
            "where request_hash = ? and status = 'stored' and cast(ingested_at_utc as date) >= ?",
            [request_hash(spec), stored_on_or_after],
        ).fetchone()[0] > 0

    def record_success(self, run_id: str, spec: RequestSpec, http_status: int, content: bytes) -> LogEntry:
        check_shape(content, spec)
        now = datetime.now(timezone.utc)
        digest = payload_hash(content)
        existing = self._existing_file(request_hash(spec), digest)
        if existing:
            return self._insert(run_id, spec, status="duplicate", http_status=http_status, digest=digest,
                                file_path=existing, size=len(content), error=None, now=now)

        window = f"{spec.window_start}_{spec.window_end}" if spec.window_start else now.strftime("%Y%m%dT%H%M%S")
        relative = Path("data/bronze") / spec.source / f"ingest_date={now.date()}" / \
            f"{spec.source}_{window}_{digest[:12]}.json"
        target = self.root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        return self._insert(run_id, spec, status="stored", http_status=http_status, digest=digest,
                            file_path=str(relative), size=len(content), error=None, now=now)

    def record_failure(self, run_id: str, spec: RequestSpec, http_status: int | None, error: str) -> LogEntry:
        return self._insert(run_id, spec, status="failed", http_status=http_status, digest=None,
                            file_path=None, size=None, error=error, now=datetime.now(timezone.utc))

    def close(self) -> None:
        self.con.close()
