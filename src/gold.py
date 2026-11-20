"""Gold marts: business-ready aggregates over silver (charter req 3).

Deterministic — no timestamps anywhere — so a rebuild is byte-identical
(proven by test). Fails loud if an upstream layer is missing: gold is never
built from nothing.
"""
from __future__ import annotations

import json
from typing import Dict

import duckdb

GOLD_DDL: Dict[str, str] = {
    "gold_latest_by_country": """
        CREATE OR REPLACE TABLE gold_latest_by_country AS
        WITH ranked AS (
            SELECT
                country_iso3, indicator_id, obs_year, value, country_name,
                row_number() OVER (
                    PARTITION BY country_iso3, indicator_id ORDER BY obs_year DESC
                ) AS rn
            FROM silver_observations
        )
        SELECT
            r.country_iso3, r.indicator_id, r.obs_year AS latest_year,
            r.value AS latest_value, r.country_name,
            c.region, c.sub_region
        FROM ranked r
        LEFT JOIN ref_countries c ON c.iso_alpha3 = r.country_iso3
        WHERE r.rn = 1
    """,
    "gold_trend": """
        CREATE OR REPLACE TABLE gold_trend AS
        SELECT
            country_iso3, indicator_id, obs_year, value,
            lag(value) OVER w AS prev_value,
            value - lag(value) OVER w AS yoy_change_abs,
            CASE
                WHEN lag(value) OVER w IS NULL OR lag(value) OVER w = 0 THEN NULL
                ELSE round(100.0 * (value - lag(value) OVER w) / lag(value) OVER w, 2)
            END AS yoy_change_pct
        FROM silver_observations
        WINDOW w AS (PARTITION BY country_iso3, indicator_id ORDER BY obs_year)
    """,
    "gold_region_aggregate": """
        CREATE OR REPLACE TABLE gold_region_aggregate AS
        SELECT
            coalesce(c.region, 'Unclassified') AS region,
            s.indicator_id,
            s.obs_year,
            count(*)    AS country_count,
            avg(s.value) AS avg_value,
            sum(s.value) AS sum_value
        FROM silver_observations s
        LEFT JOIN ref_countries c ON c.iso_alpha3 = s.country_iso3
        GROUP BY 1, 2, 3
    """,
}


def build_gold(con: duckdb.DuckDBPyConnection) -> Dict[str, int]:
    """Rebuild all gold marts. Fails loud when silver or reference is absent."""
    for table in ("silver_observations", "ref_countries"):
        exists = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
            [table],
        ).fetchone()[0]
        if not exists:
            raise ValueError(
                f"{table} is missing — build the upstream layer first "
                "(gold is never built from nothing)"
            )

    counts: Dict[str, int] = {}
    for name, ddl in GOLD_DDL.items():
        con.execute(ddl)
        counts[name] = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
    return counts
