"""Reference loader: contract-checked, normalized, joinable (charter reqs 2, 5)."""
from __future__ import annotations

import duckdb
import pytest

from src.contracts import ContractViolation
from src.reference import load_countries


def test_load_countries_from_fixture(iso3166_csv):
    con = duckdb.connect(":memory:")
    stats = load_countries(con, iso3166_csv)
    assert stats["countries"] > 200

    # empty region (Antarctica, Taiwan) is normalized to NULL, never ''
    assert con.execute(
        "SELECT count(*) FROM ref_countries WHERE region = ''"
    ).fetchone()[0] == 0
    assert con.execute(
        "SELECT count(*) FROM ref_countries WHERE region IS NULL"
    ).fetchone()[0] >= 1

    # a real ISO country joins cleanly (gold's region join depends on it)
    assert con.execute(
        "SELECT region, sub_region FROM ref_countries WHERE iso_alpha3 = 'DEU'"
    ).fetchone() == ("Europe", "Western Europe")


def test_contract_names_the_missing_column(tmp_path):
    bad = tmp_path / "bad.csv"
    bad.write_text("name,alpha-3\nNowhere,NRW\n", encoding="utf-8")  # no region column
    con = duckdb.connect(":memory:")
    with pytest.raises(ContractViolation) as excinfo:
        load_countries(con, bad)
    assert "region" in str(excinfo.value)
    # fail loud BEFORE any write
    assert not con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = 'ref_countries'"
    ).fetchone()[0]
