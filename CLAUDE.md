# Project context for AI assistants

Local medallion data pipeline on the Open-Meteo weather API for Australian locations.

* Read `.claude/skills/weather-pipeline/SKILL.md` before making changes; it holds the conventions.
* Commands (from the repo root): `make test` (offline), `make build` (dbt models and tests), `make run` (full refresh), `make ui` (Dagster).
* Config lives in `config/`. Never hardcode locations, models or variables in code.
* Times are UTC until the gold layer.
* Every change to a silver or gold model needs a matching dbt test.
* Do not commit anything under `data/` except `.gitkeep`.
* Update the skill file when a convention or command changes.
