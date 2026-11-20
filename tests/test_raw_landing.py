"""Raw landing tests: immutable rows + sha256 ingest log + byte-identical
rerun (idempotency) in DuckDB (charter req 3)."""

from __future__ import annotations

import datetime as dt

import duckdb
import pytest

from src.raw_landing import batch_sha256, ensure_schema, ingest, row_sha256, snapshot_bytes

T1 = dt.datetime(2026, 1, 1, 12, 0, 0)
T2 = dt.datetime(2026, 6, 6, 9, 30, 0)  # a *different* clock on the rerun


def _fixture_rows(wb_page1, wb_page2):
    return list(wb_page1[1]) + list(wb_page2[1])


def test_config_paths_point_at_warehouse(config):
    assert config["paths"]["warehouse"] == "warehouse/pipeline.duckdb"
    assert config["paths"]["raw_dir"] == "data/raw"


def test_ingest_lands_rows_with_timestamp_and_sha_log(tmp_path, wb_page1, wb_page2):
    db = tmp_path / "pipeline.duckdb"
    con = duckdb.connect(str(db))
    rows = _fixture_rows(wb_page1, wb_page2)
    result = ingest(con, rows, source="world_bank", now=lambda: T1)

    assert result["inserted"] == 4
    stored = con.execute(
        "SELECT row_sha256, country_iso3, obs_date, _ingested_at FROM raw_observations ORDER BY row_sha256"
    ).fetchall()
    assert len(stored) == 4
    assert all(r[3] == T1 for r in stored)
    # every stored digest matches recomputation — content addressing
    by_key = {(r["countryiso3code"], r["date"]): r for r in rows}
    for digest, iso, date, _ in stored:
        assert digest == row_sha256(by_key[(iso, date)])

    log = con.execute(
        "SELECT batch_sha256, row_count, source FROM raw_ingest_log"
    ).fetchall()
    assert log == [(batch_sha256(rows), 4, "world_bank")]
    con.close()


def test_rerun_is_byte_identical(tmp_path, wb_page1, wb_page2):
    """Idempotency: ingest the same payload twice (different clocks) and the
    raw layer snapshot — rows, timestamps, ingest log — is byte-identical."""
    db = tmp_path / "pipeline.duckdb"
    rows = _fixture_rows(wb_page1, wb_page2)

    con = duckdb.connect(str(db))
    ingest(con, rows, now=lambda: T1)
    con.close()

    con = duckdb.connect(str(db))
    snapshot_first = snapshot_bytes(con)
    # rerun with a different clock — must be a no-op, T1 preserved
    result = ingest(con, rows, now=lambda: T2)
    snapshot_second = snapshot_bytes(con)
    con.close()

    assert result["inserted"] == 0
    assert snapshot_first == snapshot_second
    assert snapshot_first  # non-empty


def test_rerun_does_not_duplicate_rows(tmp_path, wb_page1, wb_page2):
    db = tmp_path / "pipeline.duckdb"
    rows = _fixture_rows(wb_page1, wb_page2)
    con = duckdb.connect(str(db))
    ingest(con, rows, now=lambda: T1)
    ingest(con, rows, now=lambda: T2)
    ingest(con, rows, now=lambda: T2)
    count = con.execute("SELECT COUNT(*) FROM raw_observations").fetchone()[0]
    logs = con.execute("SELECT COUNT(*) FROM raw_ingest_log").fetchone()[0]
    con.close()
    assert count == 4
    assert logs == 1  # one content-addressed batch


def test_partial_overlap_only_inserts_new_rows(tmp_path, wb_page1, wb_page2):
    db = tmp_path / "pipeline.duckdb"
    con = duckdb.connect(str(db))
    first = list(wb_page1[1])
    ingest(con, first, now=lambda: T1)
    overlap = ingest(con, first, now=lambda: T2)
    assert overlap["inserted"] == 0
    total = ingest(con, _fixture_rows(wb_page1, wb_page2), now=lambda: T2)
    assert total["inserted"] == 2
    assert con.execute("SELECT COUNT(*) FROM raw_observations").fetchone()[0] == 4
    con.close()


def test_warehouse_schema_created_on_fresh_db(tmp_path):
    con = duckdb.connect(str(tmp_path / "fresh.duckdb"))
    ensure_schema(con)
    tables = {
        r[0]
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()
    }
    con.close()
    assert {"raw_observations", "raw_ingest_log"} <= tables
