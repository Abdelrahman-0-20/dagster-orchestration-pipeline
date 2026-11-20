"""World Bank Indicators — the dashboard over the gold layer.

Data resolution order (never a raw traceback):
  1. dashboard/data/*.csv — committed by CI (works on Streamlit Cloud, where
     the gitignored warehouse does not exist);
  2. the local warehouse — always fresh after a Dagster run;
  3. friendly build instructions.

Gold only: every query comes from src.export_dashboard.EXPORTS (gold_* tables).

Run locally:
    streamlit run dashboard/app.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.export_dashboard import EXPORTS  # noqa: E402  (single source of truth)

DATA_DIR = Path(__file__).resolve().parent / "data"
INDICATORS = {
    "NY.GDP.MKTP.CD": "GDP (current US\u0024)",
    "SP.POP.TOTL": "Population",
}

st.set_page_config(page_title="World Bank Indicators", page_icon="\U0001F30D", layout="wide")


def warehouse_path() -> Path:
    env = os.environ.get("DUCKDB_PATH")
    if env:
        return Path(env)
    cfg = yaml.safe_load((REPO_ROOT / "config.yaml").read_text(encoding="utf-8"))
    return REPO_ROOT / cfg["paths"]["warehouse"]


@st.cache_resource
def _con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(warehouse_path()), read_only=True)


@st.cache_data
def load(name: str) -> pd.DataFrame:
    csv = DATA_DIR / f"{name}.csv"
    if csv.exists():
        return pd.read_csv(csv)
    try:
        return pd.read_sql(EXPORTS[name], _con())
    except Exception:  # noqa: BLE001 — guidance, not a stack trace
        st.error(
            f"No data for {name}: neither dashboard/data/{name}.csv nor the "
            f"warehouse at {warehouse_path()} is available.\n\n"
            "Build it locally with:\n\n"
            "    dagster job execute -m dagster_pipeline.definitions -j daily_world_bank_refresh\n"
            "    python -m src.export_dashboard"
        )
        st.stop()
        raise  # pragma: no cover — st.stop() raises internally


def fmt(value, indicator: str) -> str:
    """Human formatting: populations grouped, GDP scaled to T/B/M with a USD sign."""
    if value is None or pd.isna(value):
        return "\u2014"
    if indicator == "SP.POP.TOTL":
        return f"{value:,.0f}"
    usd = "\u0024"
    for div, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M")):
        if abs(value) >= div:
            return f"{usd}{value / div:,.2f}{suffix}"
    return f"{usd}{value:,.0f}"


latest = load("latest")
trend = load("trend")
regions = load("regions")

st.title("World Bank Indicators")
st.caption(
    "GDP & population \u00b7 every World Bank entity, 1960\u20132025 \u00b7 "
    "raw \u2192 silver \u2192 gold via the Dagster asset graph"
)

tab_country, tab_regions, tab_rank = st.tabs(
    ["Country trends", "Regional view", "Latest ranking"]
)

with tab_country:
    c1, c2 = st.columns(2)
    indicator = c1.selectbox(
        "Indicator", list(INDICATORS), format_func=lambda k: INDICATORS[k]
    )
    countries = sorted(
        latest.loc[latest["region"].notna(), "country_name"].dropna().unique()
    )
    default = countries.index("Egypt") if "Egypt" in countries else 0
    country = c2.selectbox("Country", countries, index=default)

    iso_rows = latest.loc[latest["country_name"] == country, "country_iso3"]
    iso = iso_rows.iloc[0] if not iso_rows.empty else None
    series = trend[
        (trend["indicator_id"] == indicator) & (trend["country_iso3"] == iso)
    ].sort_values("obs_year")

    row = latest[
        (latest["country_name"] == country) & (latest["indicator_id"] == indicator)
    ]
    m1, m2, m3 = st.columns(3)
    if not row.empty:
        r = row.iloc[0]
        m1.metric("Latest value", fmt(r["latest_value"], indicator))
        hist = series[series["obs_year"] == r["latest_year"]]
        yoy = hist["yoy_change_pct"].iloc[0] if not hist.empty else None
        m2.metric(
            "Year-over-year",
            "\u2014" if yoy is None or pd.isna(yoy) else f"{yoy:+.1f}%",
        )
        m3.metric("Latest year", f"{int(r['latest_year'])}")
    if series.empty:
        st.info(f"No series for {country}.")
    else:
        st.line_chart(series.set_index("obs_year")["value"], height=340)

with tab_regions:
    r1, r2 = st.columns(2)
    indicator_r = r1.selectbox(
        "Indicator", list(INDICATORS), format_func=lambda k: INDICATORS[k], key="reg_ind"
    )
    years = sorted(
        regions.loc[regions["indicator_id"] == indicator_r, "obs_year"].unique(),
        reverse=True,
    )
    year = r2.selectbox("Year", years, index=0, key="reg_year")
    panel = regions[
        (regions["indicator_id"] == indicator_r) & (regions["obs_year"] == year)
    ].sort_values("sum_value", ascending=False)
    st.markdown(f"**Total by region \u2014 {INDICATORS[indicator_r]}, {year}**")
    st.bar_chart(panel.set_index("region")["sum_value"], height=340)
    st.dataframe(
        panel.rename(columns={
            "region": "Region", "country_count": "Entities",
            "avg_value": "Average", "sum_value": "Total",
        }).drop(columns=["indicator_id", "obs_year"]),
        use_container_width=True,
        hide_index=True,
    )

with tab_rank:
    k1, k2 = st.columns(2)
    indicator_k = k1.selectbox(
        "Indicator", list(INDICATORS), format_func=lambda k: INDICATORS[k], key="rank_ind"
    )
    hide_aggregates = k2.checkbox(
        "Hide aggregates (World, income groups\u2026)", value=True
    )
    board = latest[latest["indicator_id"] == indicator_k]
    if hide_aggregates:
        board = board[board["region"].notna()]
    board = board.sort_values("latest_value", ascending=False)
    st.markdown(f"**{INDICATORS[indicator_k]} \u2014 latest available year per entity**")
    st.dataframe(
        board.rename(columns={
            "country_name": "Entity", "country_iso3": "ISO3", "region": "Region",
            "sub_region": "Sub-region", "latest_year": "Year",
            "latest_value": INDICATORS[indicator_k],
        }),
        use_container_width=True,
        hide_index=True,
        height=440,
    )
