"""Dagster assets — the orchestration layer (charter req 4).

The asset graph encodes the PRODUCTION dependency order
(fetch -> silver -> gold), so the daily job can never run gold before silver.
Network-bound assets (fetch_world_bank, fetch_reference) are the only ones
that touch the internet; tests replace them by key with offline stubs, so the
whole graph materializes hermetically.
"""
from __future__ import annotations

import hashlib
import os
import sys
import urllib.request
from pathlib import Path

import duckdb
import yaml
from dagster import AssetKey, Backoff, DailyPartitionsDefinition, RetryPolicy, asset

ROOT = Path(__file__).resolve().parents[1]

# The dagster console script does not put the repo root on sys.path (pytest
# gets it from conftest; the CLI does not) — bootstrap it so deferred
# "from src.*" imports resolve wherever the graph runs.
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Retried with exponential backoff; raw landing is content-addressed, so a
# retry after a partial fetch re-lands only new rows (idempotent by design).
FETCH_RETRY = RetryPolicy(max_retries=3, delay=5, backoff=Backoff.EXPONENTIAL)


def load_config() -> dict:
    with open(ROOT / "config.yaml", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def warehouse_path() -> Path:
    env = os.environ.get("DUCKDB_PATH")
    if env:
        return Path(env)
    return ROOT / load_config()["paths"]["warehouse"]


def connect() -> duckdb.DuckDBPyConnection:
    path = warehouse_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _download_verified(url: str, dest: Path, expected_sha: str) -> None:
    tmp = dest.with_suffix(dest.suffix + ".part")
    with urllib.request.urlopen(url, timeout=60) as resp, open(tmp, "wb") as out:
        while True:
            chunk = resp.read(1 << 20)
            if not chunk:
                break
            out.write(chunk)
    actual = _sha256(tmp)
    if actual != expected_sha:
        tmp.unlink(missing_ok=True)
        raise ValueError(
            f"sha256 mismatch for {url}: expected {expected_sha}, got {actual} "
            "- refusing to keep a tampered file"
        )
    tmp.replace(dest)


@asset(
    description="Pull every configured World Bank indicator into raw_observations (network; retried, idempotent).",
    retry_policy=FETCH_RETRY,
)
def fetch_world_bank(context) -> dict:
    from src.raw_landing import ingest
    from src.worldbank_client import WorldBankClient

    cfg = load_config()
    wb = cfg["sources"]["world_bank"]
    client = WorldBankClient(
        max_retries=wb["retry"]["max_retries"],
        backoff_seconds=wb["retry"]["backoff_seconds"],
    )
    con = connect()
    try:
        inserted = 0
        for spec in wb["indicators"]:
            rows = client.fetch_all(spec["id"], per_page=wb["per_page"])
            inserted += ingest(con, rows, source="world_bank")["inserted"]
    finally:
        con.close()
    context.log.info(f"world_bank landing inserted {inserted} rows (reruns skip duplicates)")
    return {"inserted": inserted}


@asset(description="Download the SHA-pinned ISO 3166 CSV into data/raw (checksum-verified).")
def fetch_reference() -> dict:
    cfg = load_config()
    spec = cfg["sources"]["iso3166"]
    raw_dir = ROOT / cfg["paths"]["raw_dir"]
    raw_dir.mkdir(parents=True, exist_ok=True)
    dest = raw_dir / "iso3166.csv"
    if not dest.exists() or _sha256(dest) != spec["sha256"]:
        _download_verified(spec["url"], dest, spec["sha256"])
    return {"file": str(dest), "sha256": spec["sha256"]}


@asset(
    description="Load ref_countries from a verified copy of the pinned CSV (data/raw, else the recorded fixture — identical bytes, sha-checked).",
    deps={AssetKey("fetch_reference")},
)
def reference_countries() -> dict:
    from src.reference import load_countries

    cfg = load_config()
    spec = cfg["sources"]["iso3166"]
    candidates = [
        ROOT / cfg["paths"]["raw_dir"] / "iso3166.csv",
        ROOT / "tests" / "fixtures" / "iso3166.csv",
    ]
    chosen = next((p for p in candidates if p.exists() and _sha256(p) == spec["sha256"]), None)
    if chosen is None:
        raise ValueError(
            f"no ISO 3166 copy on disk matching sha256 {spec['sha256']} — "
            "run fetch_reference first (refuses tampered or stale files)"
        )
    con = connect()
    try:
        return load_countries(con, chosen, cfg)
    finally:
        con.close()


@asset(
    description="Rebuild silver_observations from raw (offline, byte-identical rerun).",
    deps={AssetKey("fetch_world_bank")},
)
def silver_layer() -> dict:
    from src.silver import build_silver

    con = connect()
    try:
        return build_silver(con)
    finally:
        con.close()


@asset(
    description="Rebuild the three gold marts from silver + reference (offline).",
    deps={AssetKey("silver_layer"), AssetKey("reference_countries")},
)
def gold_layer() -> dict:
    from src.gold import build_gold

    con = connect()
    try:
        return build_gold(con)
    finally:
        con.close()


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2024-01-01"),
    description="Refresh gold for the partition's year — the backfill story (idempotent).",
    deps={AssetKey("silver_layer"), AssetKey("reference_countries")},
)
def gold_backfill(context) -> dict:
    from src.gold import build_gold

    partition_year = int(context.partition_key[:4])
    con = connect()
    try:
        counts = build_gold(con)
    finally:
        con.close()
    context.log.info(f"gold refreshed for partition {context.partition_key}")
    return {"partition_year": partition_year, **counts}
