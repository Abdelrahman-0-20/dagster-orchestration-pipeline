"""Silver tests: typing, dedup rule, named contract failure, byte-identical
rebuild, lineage, count invariant (charter reqs 3 and 5)."""
from __future__ import annotations

import duckdb
import pytest

from src.contracts import ContractViolation
from src.raw_landing import ingest, row_sha256
from src.silver import build_silver, silver_snapshot


def _fixture_rows(wb_page1, wb_page2):
    return list(wb_page1[1]) + list(wb_page2[1])


def _loaded(wb_page1, wb_page2, extra=()):
    con = duckdb.connect(":memory:")
    ingest(con, list(_fixture_rows(wb_page1, wb_page2)) + list(extra))
    return con


def test_silver_types_and_shape(wb_page1, wb_page2):
    con = _loaded(wb_page1, wb_page2)
    stats = build_silver(con)
    assert stats["silver_rows"] == 4
    rows = con.execute(
        "SELECT country_iso3, indicator_id, obs_year, value, source_row_sha256 "
        "FROM silver_observations ORDER BY obs_year"
    ).fetchall()
    assert len(rows) == 4
    assert all(isinstance(r[0], str) and len(r[0]) == 3 for r in rows)
    assert all(isinstance(r[1], str) and r[1] for r in rows)
    assert all(isinstance(r[2], int) for r in rows)
    assert all(isinstance(r[3], float) for r in rows)
    assert all(isinstance(r[4], str) and len(r[4]) == 64 for r in rows)


def test_silver_dedup_keeps_smallest_sha(wb_page1, wb_page2):
    base_row = _fixture_rows(wb_page1, wb_page2)[0]
    dup = dict(base_row, value=999.0)  # same natural key, different payload
    con = _loaded(wb_page1, wb_page2, extra=[dup])
    stats = build_silver(con)
    assert stats["deduplicated"] == 1

    winner_sha = min(row_sha256(base_row), row_sha256(dup))
    winner_value = base_row["value"] if row_sha256(base_row) == winner_sha else 999.0
    stored = con.execute(
        "SELECT value, source_row_sha256 FROM silver_observations "
        "WHERE country_iso3 = ? AND indicator_id = ? AND obs_year = ?",
        [base_row["countryiso3code"], base_row["indicator"]["id"], int(base_row["date"])],
    ).fetchall()
    assert len(stored) == 1
    assert stored[0][1] == winner_sha
    assert stored[0][0] == pytest.approx(winner_value)


def test_contract_failure_is_named_and_leaves_silver_untouched(wb_page1, wb_page2):
    con = _loaded(wb_page1, wb_page2)
    build_silver(con)
    before = silver_snapshot(con)

    good = _fixture_rows(wb_page1, wb_page2)[0]
    ingest(con, [dict(good, value=None)])  # raw is faithful; silver must reject
    with pytest.raises(ContractViolation) as excinfo:
        build_silver(con)
    assert "value" in str(excinfo.value)
    assert silver_snapshot(con) == before  # nothing was written


def test_silver_rebuild_is_byte_identical(wb_page1, wb_page2):
    con = _loaded(wb_page1, wb_page2)
    build_silver(con)
    first = silver_snapshot(con)
    build_silver(con)
    assert silver_snapshot(con) == first


def test_silver_rows_trace_back_to_raw(wb_page1, wb_page2):
    con = _loaded(wb_page1, wb_page2)
    build_silver(con)
    raw_shas = {r[0] for r in con.execute("SELECT row_sha256 FROM raw_observations").fetchall()}
    silver_shas = [r[0] for r in con.execute("SELECT source_row_sha256 FROM silver_observations").fetchall()]
    assert silver_shas and set(silver_shas) <= raw_shas


def test_silver_count_equals_distinct_keys(wb_page1, wb_page2):
    con = _loaded(wb_page1, wb_page2)
    build_silver(con)
    distinct = con.execute(
        "SELECT count(*) FROM (SELECT DISTINCT payload::JSON->>'countryiso3code', "
        "payload::JSON->>'date' FROM raw_observations)"
    ).fetchone()[0]
    silver_count = con.execute("SELECT count(*) FROM silver_observations").fetchone()[0]
    assert silver_count == distinct
