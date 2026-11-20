"""Raw landing zone: immutable rows + ``_ingested_at`` + sha256 ingest log
in DuckDB at ``warehouse/pipeline.duckdb`` (charter req 3).

Idempotency model (proven by test):
- every observation row is content-addressed by ``row_sha256`` (sha256 of
  its canonical JSON) and inserted only if absent — rows are immutable;
- each batch is recorded in ``raw_ingest_log`` keyed by ``batch_sha256``
  (INSERT OR IGNORE) — a rerun with identical content writes nothing, so a
  rerun is byte-identical, including the original ``_ingested_at``.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
from typing import Dict, Iterable, Mapping, Sequence

import duckdb

SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS raw_observations (
        row_sha256     TEXT PRIMARY KEY,
        indicator_id   TEXT NOT NULL,
        country_iso3   TEXT NOT NULL,
        country_name   TEXT,
        obs_date       TEXT NOT NULL,
        value          DOUBLE,
        payload        TEXT NOT NULL,
        _ingested_at   TIMESTAMP NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS raw_ingest_log (
        batch_sha256 TEXT PRIMARY KEY,
        row_count    INTEGER NOT NULL,
        source       TEXT NOT NULL,
        ingested_at  TIMESTAMP NOT NULL
    )
    """,
)


def canonical_bytes(row: Mapping) -> bytes:
    """Deterministic serialization used for content addressing."""
    return json.dumps(
        row, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def row_sha256(row: Mapping) -> str:
    return hashlib.sha256(canonical_bytes(row)).hexdigest()


def batch_sha256(rows: Sequence[Mapping]) -> str:
    digests = sorted(row_sha256(r) for r in rows)
    return hashlib.sha256("".join(digests).encode("utf-8")).hexdigest()


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    for statement in SCHEMA:
        con.execute(statement)


def ingest(
    con: duckdb.DuckDBPyConnection,
    rows: Iterable[Mapping],
    source: str = "world_bank",
    now=None,
) -> Dict[str, int]:
    """Land raw rows immutably. Returns ``{"inserted", "skipped", "batches"}``.

    ``now`` is injectable so tests can prove a rerun preserves the original
    ``_ingested_at`` timestamps (byte-identical rerun).
    """
    clock = now or (lambda: _dt.datetime.utcnow())
    rows = list(rows)
    ensure_schema(con)

    inserted = 0
    for row in rows:
        digest = row_sha256(row)
        existing = con.execute(
            "SELECT 1 FROM raw_observations WHERE row_sha256 = ?", [digest]
        ).fetchone()
        if existing is not None:
            continue
        indicator = row.get("indicator") or {}
        country = row.get("country") or {}
        con.execute(
            "INSERT INTO raw_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                digest,
                indicator.get("id") if isinstance(indicator, Mapping) else None,
                row.get("countryiso3code"),
                country.get("value") if isinstance(country, Mapping) else None,
                row.get("date"),
                row.get("value"),
                canonical_bytes(row).decode("utf-8"),
                clock(),
            ],
        )
        inserted += 1

    # Batch log is content-addressed; INSERT OR IGNORE keeps a rerun a no-op.
    con.execute(
        "INSERT OR IGNORE INTO raw_ingest_log VALUES (?, ?, ?, ?)",
        [batch_sha256(rows), len(rows), source, clock()],
    )
    return {
        "inserted": inserted,
        "skipped": len(rows) - inserted,
        "batches": 1,
    }


def snapshot_bytes(con: duckdb.DuckDBPyConnection) -> bytes:
    """Canonical byte snapshot of the raw layer — used to prove a rerun is
    byte-identical."""
    rows = con.execute(
        "SELECT row_sha256, indicator_id, country_iso3, country_name, "
        "obs_date, value, payload, _ingested_at "
        "FROM raw_observations ORDER BY row_sha256"
    ).fetchall()
    log = con.execute(
        "SELECT batch_sha256, row_count, source, ingested_at "
        "FROM raw_ingest_log ORDER BY batch_sha256"
    ).fetchall()
    return json.dumps(
        {"rows": [list(r) for r in rows], "log": [list(l) for l in log]},
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    ).encode("utf-8")
