"""Build API request specs for each bronze source.

Every request covers all configured locations and models at once (verified in probe section 7).
The response is a list in request order, so location_ids records that order for silver.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta

from weather_pipeline.config import Settings

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
PREVIOUS_RUNS_URL = "https://previous-runs-api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

SOURCES = ("forecast", "previous_runs", "actuals")


@dataclass(frozen=True)
class RequestSpec:
    source: str
    url: str
    params: dict
    location_ids: list[str]
    models: list[str]
    window_start: date | None = None
    window_end: date | None = None
    variables: list[str] = field(default_factory=list)


def month_chunks(start: date, end: date) -> list[tuple[date, date]]:
    """Split [start, end] into calendar month windows; the first and last may be partial."""
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    chunks = []
    cursor = start
    while cursor <= end:
        next_month = (cursor.replace(day=1) + timedelta(days=32)).replace(day=1)
        chunk_end = min(next_month - timedelta(days=1), end)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def _location_params(settings: Settings) -> dict:
    return {
        "latitude": ",".join(str(loc.latitude) for loc in settings.locations),
        "longitude": ",".join(str(loc.longitude) for loc in settings.locations),
    }


def forecast_request(settings: Settings) -> RequestSpec:
    """Live forecast for the next forecast_days, scored plus display variables."""
    variables = settings.scored_variables + settings.display_variables
    return RequestSpec(
        source="forecast",
        url=FORECAST_URL,
        params={
            **_location_params(settings),
            "hourly": ",".join(variables),
            "models": ",".join(settings.models),
            "forecast_days": settings.forecast_days,
            "timezone": "GMT",
        },
        location_ids=[loc.id for loc in settings.locations],
        models=list(settings.models),
        variables=variables,
    )


def previous_runs_request(settings: Settings, start: date, end: date) -> RequestSpec:
    """Forecasts made 1..7 days before each valid hour, scored variables only."""
    variables = [f"{v}_previous_day{d}" for v in settings.scored_variables for d in settings.lead_days]
    return RequestSpec(
        source="previous_runs",
        url=PREVIOUS_RUNS_URL,
        params={
            **_location_params(settings),
            "hourly": ",".join(variables),
            "models": ",".join(settings.models),
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "timezone": "GMT",
        },
        location_ids=[loc.id for loc in settings.locations],
        models=list(settings.models),
        window_start=start,
        window_end=end,
        variables=variables,
    )


def actuals_request(settings: Settings, start: date, end: date) -> RequestSpec:
    """Truth from the pinned archive model (ERA5), scored variables only."""
    return RequestSpec(
        source="actuals",
        url=ARCHIVE_URL,
        params={
            **_location_params(settings),
            "hourly": ",".join(settings.scored_variables),
            "models": settings.actuals_model,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "timezone": "GMT",
        },
        location_ids=[loc.id for loc in settings.locations],
        models=[settings.actuals_model],
        window_start=start,
        window_end=end,
        variables=list(settings.scored_variables),
    )


def build_requests(settings: Settings, source: str, start: date, end: date) -> list[RequestSpec]:
    if source == "forecast":
        return [forecast_request(settings)]
    if source == "previous_runs":
        return [previous_runs_request(settings, s, e) for s, e in month_chunks(start, end)]
    if source == "actuals":
        return [actuals_request(settings, s, e) for s, e in month_chunks(start, end)]
    raise ValueError(f"unknown source {source!r}; expected one of {SOURCES}")
