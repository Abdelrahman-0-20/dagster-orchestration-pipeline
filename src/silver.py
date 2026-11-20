"""Silver layer: typed, conformed, deduplicated observations (charter req 3).

Silver is a deterministic function of the raw layer — no clock, no timestamps —
so rebuilding from identical raw bytes is byte-identical (proven by test).

Contract at the raw -> silver boundary (charter req 5): every raw payload is
validated with src.contracts.validate_row BEFORE anything is written, so a
violation raises the named ContractViolation and leaves any existing silver
table untouched. Fail loud, never silent drops.
"""
from __future__ import annotations

import json
from typing import Dict, Mapping, Tuple

import duckdb

from .contracts import ContractViolation, validate_row

SILVER_DDL = """
CREATE OR REPLACE TABLE silver_observations (
    country_iso3       TEXT    NOT NULL,
    indicator_id       TEXT    NOT NULL,
    obs_year           INTEGER NOT NULL,
    country_name       TEXT,
    value              DOUBLE,
    source_row_sha256  TEXT    NOT NULL,
    PRIMARY KEY (country_iso3, indicator_id, obs_year)
)
"""


def _project(payload: Mapping, source_row_sha256: str) -> Tuple:
    """Validate (named failure) + type + conformance for one raw payload."""
    validate_row("world_bank", payload)
    try:
        obs_year = int(str(payload["date"]).strip())
    except ValueError:
        raise ContractViolation(
            "world_bank", "date", f"expected a year, got {payload['date']!r}"
        ) from None
    country = payload.get("country") or {}
    indicator = payload.get("indicator") or {}
    name = country.get("value")
    return (
        str(payload["countryiso3code"]).strip(),
        str(indicator["id"]).strip(),
        obs_year,
        name.strip() if isinstance(name, str) and name.strip() else None,
        float(payload["value"]),
        source_row_sha256,
    )


def build_silver(con: duckdb.DuckDBPyConnection) -> Dict[str, int]:
    """Rebuild silver_observations from raw_observations.

    Phase 1 validates EVERY payload (fail loud before any write); phase 2
    deduplicates deterministically — one row per natural key
    (country_iso3, indicator_id, obs_year), keeping the smallest
    source_row_sha256 lexicographically — then replaces the table.
    """
    raw = con.execute(
        "SELECT row_sha256, payload FROM raw_observations ORDER BY row_sha256"
    ).fetchall()

    projected = [_project(json.loads(payload), sha) for sha, payload in raw]

    deduped: Dict[Tuple, Tuple] = {}
    for row in projected:
        key = row[:3]
        current = deduped.get(key)
        if current is None or row[5] < current[5]:
            deduped[key] = row

    rows = sorted(deduped.values())
    con.execute(SILVER_DDL)
    con.executemany(
        "INSERT INTO silver_observations VALUES (?, ?, ?, ?, ?, ?)", rows
    )
    return {
        "raw_rows": len(raw),
        "silver_rows": len(rows),
        "deduplicated": len(raw) - len(rows),
    }


def silver_snapshot(con: duckdb.DuckDBPyConnection) -> bytes:
    """Canonical byte snapshot of silver, ordered by natural key."""
    rows = con.execute(
        "SELECT country_iso3, indicator_id, obs_year, country_name, value, "
        "source_row_sha256 FROM silver_observations "
        "ORDER BY country_iso3, indicator_id, obs_year"
    ).fetchall()
    return json.dumps(
        [list(r) for r in rows],
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    ).encode("utf-8")
