"""Load pipeline and location settings from config/. Nothing else in the package hardcodes them."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import yaml


@dataclass(frozen=True)
class Location:
    id: str
    name: str
    state: str
    latitude: float
    longitude: float
    timezone: str


@dataclass(frozen=True)
class Settings:
    root: Path
    models: list[str]
    forecast_days: int
    lead_days: list[int]
    backfill_start: date
    actuals_model: str
    actuals_lookback_days: int
    scored_variables: list[str]
    display_variables: list[str]
    pause_between_requests_seconds: float
    locations: list[Location]

    @property
    def bronze_dir(self) -> Path:
        return self.root / "data" / "bronze"

    @property
    def warehouse_path(self) -> Path:
        return self.root / "data" / "warehouse.duckdb"


def load_settings(root: Path | None = None) -> Settings:
    root = (root or Path.cwd()).resolve()
    pipeline = yaml.safe_load((root / "config" / "pipeline.yaml").read_text())
    raw_locations = yaml.safe_load((root / "config" / "locations.yaml").read_text())["locations"]
    return Settings(
        root=root,
        models=list(pipeline["models"]),
        forecast_days=int(pipeline["forecast_days"]),
        lead_days=list(pipeline["lead_days"]),
        backfill_start=date.fromisoformat(str(pipeline["backfill_start"])),
        actuals_model=pipeline["actuals"]["model"],
        actuals_lookback_days=int(pipeline["actuals"]["lookback_days"]),
        scored_variables=list(pipeline["scored_variables"]),
        display_variables=list(pipeline["display_variables"]),
        pause_between_requests_seconds=float(pipeline.get("pause_between_requests_seconds", 0)),
        locations=[Location(**loc) for loc in raw_locations],
    )
