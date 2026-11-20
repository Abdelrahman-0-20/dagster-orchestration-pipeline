# dagster-orchestration-pipeline

[![verify](https://github.com/Abdelrahman-0-20/dagster-orchestration-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Abdelrahman-0-20/dagster-orchestration-pipeline/actions/workflows/ci.yml)

Project #4 of a [data engineering portfolio](https://github.com/Abdelrahman-0-20/api-ingestion-pipeline)
(companions: [API ingestion](https://github.com/Abdelrahman-0-20/api-ingestion-pipeline) ·
[analytics engineering](https://github.com/Abdelrahman-0-20/analytics-engineering-pipeline) ·
[dbt + DuckDB ELT](https://github.com/Abdelrahman-0-20/energy-analytics-dbt)).

**The orchestration axis:** [World Bank development indicators](https://api.worldbank.org)
(no API key) → a **Dagster asset graph** (daily schedule, filesystem sensor,
exponential-backoff retries, daily partitions for backfills) → typed layers in
**DuckDB** → gold marts — proven by **40 pytest tests that never touch the
network**, gated by `./verify.sh`, run by **GitHub Actions**. No cloud
accounts, no API keys, no docker: `pip install` and `dagster dev`.

**At a glance (verified against the live API):**35,112 raw rows →31,743
silver (3,369 policy-dropped: absent measurements + no-ISO entities, counted
loudly) →519 latest-value rows and792 region aggregates across6 regions ·
re-ingest inserted0 duplicates (production idempotency) · named-error contract
at the boundary · SHA-pinned reference data (checksum-verified before load) ·
asset graph order encoded in Dagster deps (gold can never run before silver).

## Architecture

```
api.worldbank.org (paginated, retried)          ISO 3166 CSV (SHA-pinned)
        │ fetch_world_bank (RetryPolicy×3)             │ fetch_reference
        ▼                                              ▼
  raw_observations                              data/raw/iso3166.csv
  (content-addressed, ingest log,                      │ reference_countries
   byte-identical rerun)                               │ (sha-verified copy)
        │ silver_layer                                       │
        ▼                                                    │
  silver_observations  ◄──── named ContractViolation ────────┤
  (typed, conformed, deduped)    never silent drops          │
        ├─────────────────────────────────────────────────────┘
        ▼
  gold_latest_by_country · gold_trend · gold_region_aggregate
        ▲
        └── gold_backfill (daily-partitioned — the backfill story)

dagster_pipeline: defs = Definitions(assets ×6, job, daily schedule, sensor)
tests/          :40 hermetic tests (fixtures recorded once, zero network)
```

## Run it

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

./verify.sh                        # the gate: pytest + config/CI parse + compileall
dagster dev                        # UI + daemon on localhost:3000
dagster job execute -m dagster_pipeline.definitions -j daily_world_bank_refresh
# the daily refresh: fetch -> reference -> silver -> gold (verified end-to-end
# against the live World Bank API; gold_backfill is partition-only, excluded)
```

Backfills: select a partition in the Dagster UI (gold_backfill is
daily-partitioned), or from code:

```python
from dagster import materialize
from dagster_pipeline.assets import gold_backfill
materialize([gold_backfill], partition_key="2024-01-01")
```

## Testing philosophy: fixtures, not the network

The World Bank responses used by tests are **recorded once** into
`tests/fixtures/` and served through an injected fetch function — every test
runs offline and deterministically. Dagster's network assets are replaced
**by key** with stubs in tests, so the real dependency graph (including
ordering) is exercised without a single HTTP call.

## Design decisions (interview talking points)

| Decision | Why |
|---|---|
| Asset deps encode production order | gold can never race silver; the daily job follows the graph, not hope |
| Network assets stubbed **by key** in tests | the real DAG is tested offline — stubs plug into the same dependency slots |
| Content-addressed raw layer | retries are safe (idempotent), reruns are byte-identical (proven by test) |
| Contract at raw → silver with a **named** error | violations fail loud and leave the previous silver untouched — never silent drops |
| Silver has no timestamps | silver is a deterministic function of raw → byte-identical rebuild without clock injection |
| SHA-pinned reference data | the ISO CSV is checksum-verified before load; tampered or stale files are refused |
| Serial executor (max_concurrent=1) | DuckDB is single-writer — parallel step workers fought over the file lock in production; serial execution is correct and the warehouse is tiny, so latency is free |
| Absent measurement ≠ contract violation | 35k real rows taught this: 2,841 null values and 660 no-ISO entities are policy drops with loud counts — the contract checks structure, silver applies policy |
| `RetryPolicy` ×3 exponential on the fetch asset | transient API failures heal themselves; the landing layer makes retries free |
| Daily partitions on gold_backfill | backfills become first-class: re-run any window without touching the rest |
| Curated entity facts stay in `config.yaml` | thresholds and contracts are data, not code |

## Resume line

> Built an orchestration-layer pipeline (Python + Dagster + DuckDB): a
> dependency-ordered asset graph over the World Bank API with retries, a daily
> schedule, a filesystem sensor and daily-partitioned backfills; typed
> raw→silver→gold layers with a named-error data contract;40 hermetic tests
> (fixtures, zero network), one `./verify.sh` gate, GitHub Actions on every push.

## Roadmap

- Publish a live demo once the repo is on GitHub (CI badge turns green on first push)
- `dagster dev` screenshots in this README
- Wire the sensor's RunRequests to land incoming files (currently trigger-only)
- dbt on top of the gold layer (port project #3's pattern)
