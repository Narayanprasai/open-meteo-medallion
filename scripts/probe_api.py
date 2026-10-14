"""One off probe of the Open-Meteo APIs before building the ingester.

Answers:
  1. Which candidate models return Previous Runs data for Australia, and at which lead days?
  2. Is a model missing from the archive but available live in the Forecast API?
  3. How far back does each model's archive go (is January 2024 populated)?
  4. How are response keys named when several models and locations are requested at once?
  5. How far behind today is the ERA5 archive (the truth source)?

Run: uv run python scripts/probe_api.py            (all sections)
     uv run python scripts/probe_api.py --only 3,5   (selected sections)
"""

from __future__ import annotations

import argparse

from datetime import date, timedelta
from pathlib import Path

import httpx
import yaml

ROOT = Path(__file__).resolve().parents[1]
PIPELINE = yaml.safe_load((ROOT / "config/pipeline.yaml").read_text())
LOCATIONS = {l["id"]: l for l in yaml.safe_load((ROOT / "config/locations.yaml").read_text())["locations"]}

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
PREVIOUS_RUNS_URL = "https://previous-runs-api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

PROBE_LOCATIONS = ["syd", "per", "drw"]  # east coast DST, west no DST, tropical half hour offset
LEADS = PIPELINE["lead_days"]
# Configured models first, then global models that cover Australia.
CANDIDATE_MODELS = list(dict.fromkeys(
    PIPELINE["models"] + ["ecmwf_ifs", "gfs_global", "icon_global", "jma_gsm", "gem_global"]
))


def get(client: httpx.Client, url: str, params: dict) -> dict | list | None:
    """Return parsed JSON, or None after printing the reason on any failure."""
    try:
        r = client.get(url, params=params)
        body = r.json()
    except (httpx.HTTPError, ValueError) as exc:
        print(f"  REQUEST FAILED: {type(exc).__name__}: {exc}")
        return None
    if r.status_code != 200 or (isinstance(body, dict) and body.get("error")):
        reason = body.get("reason") if isinstance(body, dict) else body
        print(f"  ERROR {r.status_code}: {reason}")
        return None
    return body


def non_null(values: list) -> int:
    return sum(v is not None for v in values)


def probe_lead_coverage(client: httpx.Client) -> None:
    print("\n== 1: Previous Runs coverage, last 7 days, temperature_2m ==")
    print(f"  columns: hours, day0, {', '.join(f'd{d}' for d in LEADS)}")
    variables = ["temperature_2m"] + [f"temperature_2m_previous_day{d}" for d in LEADS]
    for model in CANDIDATE_MODELS:
        for loc_id in PROBE_LOCATIONS:
            loc = LOCATIONS[loc_id]
            body = get(client, PREVIOUS_RUNS_URL, {
                "latitude": loc["latitude"], "longitude": loc["longitude"],
                "hourly": ",".join(variables), "models": model,
                "past_days": 7, "forecast_days": 1, "timezone": "GMT",
            })
            if body is None:
                print(f"  {model:<18} {loc_id}: no response")
                continue
            hourly = body["hourly"]
            counts = " ".join(f"{non_null(hourly.get(v, [])):>3}" for v in variables)
            print(f"  {model:<18} {loc_id}: {len(hourly['time']):>3} | {counts} "
                  f"| grid=({body['latitude']:.3f},{body['longitude']:.3f}) elev={body.get('elevation')}")


def probe_live_forecast(client: httpx.Client) -> None:
    print("\n== 2: live Forecast API, next 7 days, Sydney ==")
    loc = LOCATIONS["syd"]
    for model in CANDIDATE_MODELS:
        body = get(client, FORECAST_URL, {
            "latitude": loc["latitude"], "longitude": loc["longitude"],
            "hourly": "temperature_2m", "models": model,
            "forecast_days": 7, "timezone": "GMT",
        })
        if body is not None:
            values = body["hourly"]["temperature_2m"]
            print(f"  {model:<18} non null {non_null(values):>3} of {len(values)}")


DEPTH_DATES = ["2024-07-01", "2025-01-01", "2025-04-01", "2025-07-01",
               "2025-10-01", "2026-01-01", "2026-04-01", "2026-07-01"]
DEPTH_MODELS = ["ecmwf_ifs025", "ecmwf_ifs", "gfs_global", "gem_global", "jma_gsm"]


def probe_archive_depth(client: httpx.Client) -> None:
    """Hours (of 24) with a previous_day7 value on each date: shows where each archive starts."""
    print("\n== 3: Previous Runs archive depth, Sydney, previous_day7 hours per date (24 = full) ==")
    print(f"  {'model':<14} " + " ".join(d[:7] for d in DEPTH_DATES))
    loc = LOCATIONS["syd"]
    for model in DEPTH_MODELS:
        cells = []
        for day in DEPTH_DATES:
            body = get(client, PREVIOUS_RUNS_URL, {
                "latitude": loc["latitude"], "longitude": loc["longitude"],
                "hourly": "temperature_2m_previous_day7", "models": model,
                "start_date": day, "end_date": day, "timezone": "GMT",
            })
            cells.append("err" if body is None else str(non_null(body["hourly"]["temperature_2m_previous_day7"])))
        print(f"  {model:<14} " + " ".join(f"{c:>7}" for c in cells))


def probe_multi_model_keys(client: httpx.Client, models: list[str]) -> None:
    print(f"\n== 4: key naming with models {models} and two locations in one request ==")
    locs = [LOCATIONS[i] for i in PROBE_LOCATIONS[:2]]
    body = get(client, PREVIOUS_RUNS_URL, {
        "latitude": ",".join(str(l["latitude"]) for l in locs),
        "longitude": ",".join(str(l["longitude"]) for l in locs),
        "hourly": "temperature_2m,temperature_2m_previous_day1",
        "models": ",".join(models),
        "past_days": 1, "forecast_days": 1, "timezone": "GMT",
    })
    if body is None:
        return
    print(f"  response type: {type(body).__name__}, items: {len(body) if isinstance(body, list) else 1}")
    first = body[0] if isinstance(body, list) else body
    print(f"  top level keys: {sorted(first.keys())}")
    print(f"  hourly keys: {sorted(first['hourly'].keys())}")


def probe_archive_lag(client: httpx.Client) -> None:
    """Default archive model fills recent days with forecasts; pure ERA5 lags ~5 days."""
    print("\n== 5: archive lag per truth model (last non null hour, UTC) ==")
    loc = LOCATIONS["syd"]
    end = date.today()
    for model in ["best_match", "era5", "era5_land"]:
        body = get(client, ARCHIVE_URL, {
            "latitude": loc["latitude"], "longitude": loc["longitude"],
            "hourly": "temperature_2m", "models": model, "timezone": "GMT",
            "start_date": (end - timedelta(days=14)).isoformat(), "end_date": end.isoformat(),
        })
        if body is None:
            continue
        pairs = list(zip(body["hourly"]["time"], body["hourly"]["temperature_2m"]))
        last = max((t for t, v in pairs if v is not None), default=None)
        print(f"  {model:<12} last non null: {last}; requested up to {end}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", default="1,2,3,4,5", help="comma separated sections to run, e.g. 3,5")
    sections = set(parser.parse_args().only.split(","))
    with httpx.Client(timeout=30) as client:
        if "1" in sections:
            probe_lead_coverage(client)
        if "2" in sections:
            probe_live_forecast(client)
        if "3" in sections:
            probe_archive_depth(client)
        if "4" in sections:
            probe_multi_model_keys(client, ["ecmwf_ifs025", "gfs_global"])
        if "5" in sections:
            probe_archive_lag(client)


if __name__ == "__main__":
    main()
