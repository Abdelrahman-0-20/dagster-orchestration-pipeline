"""Dashboard export: CSVs match gold and are byte-deterministic."""
from __future__ import annotations

import duckdb
import pandas as pd

from src.export_dashboard import EXPORTS, export
from src.gold import build_gold
from src.reference import load_countries
from src.raw_landing import ingest
from src.silver import build_silver


def _seed(wb_page1, wb_page2, iso3166_csv):
    con = duckdb.connect(":memory:")
    ingest(con, list(wb_page1[1]) + list(wb_page2[1]))
    build_silver(con)
    load_countries(con, iso3166_csv)
    build_gold(con)
    return con


def test_export_matches_gold(wb_page1, wb_page2, iso3166_csv, tmp_path):
    con = _seed(wb_page1, wb_page2, iso3166_csv)
    counts = export(con, export_dir=tmp_path)
    gold_tables = {
        "latest": "gold_latest_by_country",
        "trend": "gold_trend",
        "regions": "gold_region_aggregate",
    }
    assert set(counts) == set(EXPORTS) == set(gold_tables)
    for name, table in gold_tables.items():
        path = tmp_path / f"{name}.csv"
        assert path.is_file()
        df = pd.read_csv(path)
        gold_rows = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        assert len(df) == counts[name] == gold_rows


def test_export_is_byte_deterministic(wb_page1, wb_page2, iso3166_csv, tmp_path):
    con = _seed(wb_page1, wb_page2, iso3166_csv)
    export(con, export_dir=tmp_path / "a")
    export(con, export_dir=tmp_path / "b")
    for name in EXPORTS:
        assert (tmp_path / "a" / f"{name}.csv").read_bytes() == (
            tmp_path / "b" / f"{name}.csv"
        ).read_bytes()
