# dagster-orchestration-pipeline — charter (M0)

**Project #4 of the data engineering portfolio — the orchestration axis.**
A Dagster asset pipeline over **World Bank development indicators** (no API key)
→ typed layers in **DuckDB** → gold marts, proven by **pytest**, scheduled by
**Dagster**, run by **GitHub Actions**. No cloud accounts, no docker required:
Dagster installs with pip and `dagster dev` serves the UI locally.

> This charter is the spec. All code comes from the relay
> (Planner → Builder → Verifier), one milestone per run.

## Requirements

1. **Layout:** `config.yaml` (sources + contracts + paths — thresholds are
   data, never code), `src/` (pure functions + API client),
   `dagster_pipeline/` (assets, schedules, sensors, retry policy),
   `tests/` with `tests/fixtures/`, `.github/workflows/ci.yml`,
   `./verify.sh`, `README.md`, `requirements.txt`; `.venv/`, `warehouse/`,
   `__pycache__/` gitignored.
2. **Sources:** World Bank API v2 (`api.worldbank.org/v2`, **no key**) —
   ≥2 indicators × all countries, **paginated** — plus one **SHA-pinned static
   CSV** (ISO 3166 country → region from raw.githubusercontent.com). Tests are
   **zero-network**: API responses served from `tests/fixtures/` (recorded
   JSON) through an injected fetch function.
3. **Warehouse:** DuckDB at `warehouse/pipeline.duckdb` with layers
   `raw_*` (immutable landing + ``_ingested_at`` + sha256 ingest log — a
   rerun is byte-identical) → `silver_*` (typed, conformed, deduplicated) →
   `gold_*` (marts: latest value per country, trends, regional aggregates).
4. **Dagster primitives (all four, defined and tested):** an `@asset` graph
   mirroring the layers (raw → silver → gold); a **`@daily` schedule**; one
   **sensor** (filesystem watch on `data/incoming/`); an explicit
   **RetryPolicy** on the API asset; one **`@daily_partitioned` asset**
   (the backfill story).
5. **Data contract:** required fields per source enforced at the raw → silver
   boundary; violations fail loud with named errors (never silent drops).
6. **Tests: ≥25, hermetic (no network)** — contract, cleaning, idempotent
   rerun, gold invariants (ranges/uniqueness/referential integrity), asset
   materialization via `materialize()`, and existence/shape of schedule,
   sensor, retry policy, partition.
7. **`./verify.sh`** exits 0: `python -m pytest -q` (bare, no masking pipes)
   + YAML parse of config + CI workflow + `python -m compileall -q src
   dagster_pipeline`. Runs from repo root against `.venv`.
8. **CI:** GitHub Actions on push/PR + weekly cron builds `.venv` from
   `requirements.txt` and runs `./verify.sh`; badge in README.
9. **README:** architecture diagram, "Design decisions (interview talking
   points)" table, resume line, run instructions, badge placeholders
   (repo not yet created on GitHub — no live URLs).
10. **Commits:** conventional; **each milestone ends with exactly one commit**
    (`M1: …`, `M2: …`, `M3: …`) and a clean `git status`.

## Milestones

| Milestone | Scope | Gate |
|---|---|---|
| **M1a** | skeleton + config + World Bank client (pagination, retry, fixture-injectable — `tests/fixtures/` already recorded) + raw landing (ingest log, idempotent); **≥6 tests** | `./verify.sh` exit 0, one commit |
| **M1b** | silver transform + named-error data contract at raw→silver; **≥12 tests total** | `./verify.sh` exit 0, one commit |

> *Amendment (post-M0): M1 split into M1a/M1b so each relay cycle fits the
> orchestrator's 10-minute window (planner runs on pro/high and consumes ~half
> of it). Same scope, two gates.*
| **M2** | gold marts + contract/quality tests + full Dagster graph (assets, daily schedule, sensor, RetryPolicy, partitioned asset); **≥25 tests total** | `./verify.sh` exit 0, one commit |
| **M3** | CI workflow + README (architecture, decisions, resume line) + polish | `./verify.sh` exit 0, one commit; **no push without human instruction** |

## Gate

`./verify.sh` must exit 0. Verification commands run bare (the last segment of
the command owns the exit code).
