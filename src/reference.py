"""ISO 3166 reference: country -> region (charter req 2's SHA-pinned CSV).

Contract-checked with the named ContractViolation BEFORE any write (charter
req 5). Empty region strings are legitimate — UN M49 leaves a few entities
unclassified — so presence is validated first, then '' is normalized to NULL.
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Mapping

import duckdb

from .contracts import validate_row

DDL = """
CREATE OR REPLACE TABLE ref_countries (
    country_name TEXT NOT NULL,
    iso_alpha2   TEXT,
    iso_alpha3   TEXT NOT NULL,
    region       TEXT,
    sub_region   TEXT,
    PRIMARY KEY (iso_alpha3)
)
"""


def _norm(value) -> str | None:
    value = (value or "").strip()
    return value or None


def load_countries(
    con: duckdb.DuckDBPyConnection,
    csv_path: Path | str,
    config: Mapping | None = None,
) -> Dict[str, int]:
    """Load the pinned ISO CSV into ref_countries; validate, then write."""
    with open(csv_path, newline="", encoding="utf-8-sig") as fh:
        rows = list(csv.DictReader(fh))

    for row in rows:  # phase 1: contract for EVERY row before any write
        validate_row("iso3166", row, config)

    con.execute(DDL)  # phase 2: conformed load
    con.executemany(
        "INSERT INTO ref_countries VALUES (?, ?, ?, ?, ?)",
        [
            (
                _norm(r.get("name")),
                _norm(r.get("alpha-2")),
                _norm(r.get("alpha-3")),
                _norm(r.get("region")),
                _norm(r.get("sub-region")),
            )
            for r in rows
        ],
    )
    return {"countries": len(rows)}
