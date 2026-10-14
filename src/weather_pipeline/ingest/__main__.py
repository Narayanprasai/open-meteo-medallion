"""Command line entry point for bronze ingestion.

Run from the repository root:
  uv run python -m weather_pipeline.ingest                                  # incremental, all sources
  uv run python -m weather_pipeline.ingest --source forecast                # live 7 day forecast only
  uv run python -m weather_pipeline.ingest --source previous_runs actuals \\
      --start 2025-10-01 --end 2026-10-14                                    # backfill
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from weather_pipeline.config import load_settings
from weather_pipeline.ingest.run import SOURCES, run_ingestion


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="weather_pipeline.ingest", description="Load Open-Meteo data into bronze.")
    parser.add_argument("--source", nargs="+", choices=SOURCES, default=list(SOURCES))
    parser.add_argument("--start", type=date.fromisoformat, help="UTC start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=date.fromisoformat, help="UTC end date (YYYY-MM-DD)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # hide one long URL line per request
    summary = run_ingestion(load_settings(), args.source, args.start, args.end)
    return 0 if summary.ok else 1


if __name__ == "__main__":
    sys.exit(main())
