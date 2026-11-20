"""Dagster graph: shape, retry policy, partitions, offline materialization, sensor."""
from __future__ import annotations

import duckdb
from dagster import (
    AssetKey,
    DailyPartitionsDefinition,
    asset as dagster_asset,
    build_sensor_context,
    materialize,
)

from dagster_pipeline import assets as A
from dagster_pipeline.definitions import defs
from dagster_pipeline.sensors import incoming_files_sensor
from src.raw_landing import ingest


def _asset(key: str):
    return next(a for a in defs.assets if a.key == AssetKey(key))


def test_definitions_shape():
    assert {a.key for a in defs.assets} == {
        AssetKey(k)
        for k in (
            "fetch_world_bank",
            "fetch_reference",
            "reference_countries",
            "silver_layer",
            "gold_layer",
            "gold_backfill",
        )
    }

    schedules = list(defs.schedules)
    assert len(schedules) == 1
    assert schedules[0].name == "daily_6am_utc"
    assert schedules[0].cron_schedule == "0 6 * * *"

    sensors = list(defs.sensors)
    assert len(sensors) == 1
    assert sensors[0].name == "incoming_files"

    assert any(j.name == "daily_world_bank_refresh" for j in defs.jobs)


def test_fetch_asset_carries_exponential_retry():
    assert A.FETCH_RETRY.max_retries == 3
    retry = _asset("fetch_world_bank").op.retry_policy
    assert retry is not None, "retry policy must be attached to the asset's op"
    assert retry.max_retries == 3


def test_asset_graph_encodes_production_order():
    silver = _asset("silver_layer")
    assert AssetKey("fetch_world_bank") in silver.dependency_keys
    gold = _asset("gold_layer")
    assert AssetKey("silver_layer") in gold.dependency_keys
    assert AssetKey("reference_countries") in gold.dependency_keys


def test_gold_backfill_is_daily_partitioned():
    assert isinstance(_asset("gold_backfill").partitions_def, DailyPartitionsDefinition)


def test_offline_materialization(tmp_path, monkeypatch, wb_page1, wb_page2):
    """The whole graph runs hermetically: network fetches replaced by stubs."""
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "pipeline.duckdb"))

    @dagster_asset(key="fetch_world_bank")
    def stub_fetch():
        return {"inserted": 4}

    @dagster_asset(key="fetch_reference")
    def stub_reference():
        return {"countries": 0}

    con = duckdb.connect(str(tmp_path / "pipeline.duckdb"))
    ingest(con, list(wb_page1[1]) + list(wb_page2[1]))
    con.close()

    result = materialize(
        [stub_fetch, stub_reference, A.reference_countries, A.silver_layer, A.gold_layer]
    )
    assert result.success, result

    partitioned = materialize(
        [stub_fetch, stub_reference, A.reference_countries, A.silver_layer, A.gold_backfill],
        partition_key="2024-01-01",
    )
    assert partitioned.success, partitioned


def test_sensor_yields_run_request_per_new_file(tmp_path, monkeypatch):
    monkeypatch.setenv("INCOMING_DIR", str(tmp_path))

    ctx = build_sensor_context(cursor="")
    assert list(incoming_files_sensor(ctx)) == []  # empty dir -> SkipReason

    (tmp_path / "day1.csv").write_text("a,b\n", encoding="utf-8")
    events = list(incoming_files_sensor(ctx))
    assert len(events) == 1
    assert events[0].run_key == "day1.csv"

    resumed = build_sensor_context(cursor=ctx.cursor)
    assert list(incoming_files_sensor(resumed)) == []  # cursor dedupes
