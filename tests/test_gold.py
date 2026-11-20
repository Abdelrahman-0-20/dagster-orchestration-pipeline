"""Gold marts: latest, trend math, region aggregates, fail-loud, idempotent."""
from __future__ import annotations

import json

import duckdb
import pytest

from src.gold import build_gold
from src.reference import load_countries
from src.raw_landing import ingest
from src.silver import build_silver


def _seed(wb_page1, wb_page2, iso3166_csv):
    con = duckdb.connect(":memory:")
    ingest(con, list(wb_page1[1]) + list(wb_page2[1]))
    build_silver(con)
    load_countries(con, iso3166_csv)
    return con


def _snapshot(con) -> bytes:
    # ORDER BY every table: unordered SELECTs are never a stable basis for a
    # byte-comparison (row order may differ between identical rebuilds).
    order = {
        "gold_latest_by_country": "country_iso3, indicator_id",
        "gold_trend": "country_iso3, indicator_id, obs_year",
        "gold_region_aggregate": "region, indicator_id, obs_year",
    }
    out = {}
    for table, cols in order.items():
        rows = con.execute(f"SELECT * FROM {table} ORDER BY {cols}").fetchall()
        out[table] = [list(r) for r in rows]
    return json.dumps(out, sort_keys=True, default=str, separators=(",", ":")).encode()


def test_gold_refuses_to_build_from_nothing():
    con = duckdb.connect(":memory:")
    with pytest.raises(ValueError, match="silver_observations is missing"):
        build_gold(con)


def test_latest_by_country(wb_page1, wb_page2, iso3166_csv):
    con = _seed(wb_page1, wb_page2, iso3166_csv)
    counts = build_gold(con)
    assert counts["gold_latest_by_country"] == 1  # fixtures contain one entity

    latest_year, latest_value, region = con.execute(
        "SELECT latest_year, latest_value, region FROM gold_latest_by_country"
    ).fetchone()
    assert latest_year == 2025
    assert latest_value == pytest.approx(1358694050872.54)
    assert region is None  # AFE is a World Bank aggregate, not an ISO country


def test_trend_math(wb_page1, wb_page2, iso3166_csv):
    con = _seed(wb_page1, wb_page2, iso3166_csv)
    build_gold(con)
    rows = con.execute(
        "SELECT obs_year, prev_value, yoy_change_abs FROM gold_trend ORDER BY obs_year"
    ).fetchall()
    by_year = {r[0]: r for r in rows}
    assert by_year[2023][1] == pytest.approx(1226461319482.05)   # prev = 2022
    assert by_year[2023][2] == pytest.approx(1179121535535.41 - 1226461319482.05)
    assert by_year[2022][1] is None  # first year has no predecessor


def test_region_aggregate_joins_real_countries(wb_page1, wb_page2, iso3166_csv):
    con = _seed(wb_page1, wb_page2, iso3166_csv)
    # inject a real ISO country so the reference join is actually exercised
    con.execute(
        "INSERT INTO silver_observations VALUES "
        "('DEU', 'TEST.IND', 2024, 'Germany', 10.0, ?)",
        ["a" * 64],
    )
    build_gold(con)
    regions = {
        r[0]
        for r in con.execute("SELECT DISTINCT region FROM gold_region_aggregate").fetchall()
    }
    assert regions == {"Europe", "Unclassified"}  # AFE stays unclassified
    europe = con.execute(
        "SELECT country_count FROM gold_region_aggregate WHERE region = 'Europe'"
    ).fetchone()
    assert europe[0] == 1


def test_gold_rebuild_is_byte_identical(wb_page1, wb_page2, iso3166_csv):
    con = _seed(wb_page1, wb_page2, iso3166_csv)
    build_gold(con)
    first = _snapshot(con)
    build_gold(con)
    assert _snapshot(con) == first
