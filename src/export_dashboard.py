"""Export the gold marts to dashboard/data/*.csv — the dashboard's data layer.

Streamlit Community Cloud has no warehouse (warehouse/*.duckdb is gitignored:
it is a build artifact). CI runs the production job, then this exporter, and
commits the CSVs back with [skip ci]; locally the dashboard prefers the live
warehouse and falls back to these CSVs.

EXPORTS is the single source of truth: the app's warehouse fallback runs the
exact same queries this exporter materializes.

    python -m src.export_dashboard
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR = REPO_ROOT / "dashboard" / "data"


def load_config() -> dict:
    with open(REPO_ROOT / "config.yaml", encoding="utf-8") as fh:
        return yaml.safe_load(fh)

# Deterministic ORDER BYs: byte-identical CSVs across runs (no churn commits).
EXPORTS: dict[str, str] = {
    "latest": (
        "SELECT * FROM gold_latest_by_country "
        "ORDER BY indicator_id, latest_value DESC NULLS LAST, country_iso3"
    ),
    "trend": (
        "SELECT * FROM gold_trend "
        "ORDER BY indicator_id, country_iso3, obs_year"
    ),
    "regions": (
        "SELECT * FROM gold_region_aggregate "
        "ORDER BY indicator_id, obs_year DESC, region"
    ),
}


def export(
    con: duckdb.DuckDBPyConnection, export_dir: Path = EXPORT_DIR
) -> dict[str, int]:
    export_dir.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    for name, sql in EXPORTS.items():
        df = pd.read_sql(sql, con)
        df.to_csv(export_dir / f"{name}.csv", index=False)
        counts[name] = len(df)
    return counts


def main(argv: list[str] | None = None) -> int:
    cfg = load_config()
    warehouse = (argv or sys.argv[1:] or [str(REPO_ROOT / cfg["paths"]["warehouse"])])[0]
    if not Path(warehouse).exists():
        print(
            f"[export] ERROR: warehouse not found at {warehouse} — "
            "run: dagster job execute -m dagster_pipeline.definitions -j daily_world_bank_refresh",
            file=sys.stderr,
        )
        return 1
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        counts = export(con)
    finally:
        con.close()
    for name, n in counts.items():
        print(f"[export] dashboard/data/{name}.csv: {n} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
