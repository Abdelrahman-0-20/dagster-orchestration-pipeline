"""Daily job + schedule (charter req 4)."""
from dagster import (
    AssetSelection,
    ScheduleDefinition,
    define_asset_job,
    multiprocess_executor,
)

# DuckDB is single-writer: parallel step workers would fight over the file
# lock, so the job runs its steps serially (max_concurrent=1). The warehouse
# is tiny — latency is a non-issue, correctness is not.
single_writer_executor = multiprocess_executor.configured(
    {"max_concurrent": 1}, name="single_writer"
)

# The daily refresh covers the pipeline proper. gold_backfill is EXCLUDED:
# it is a partition-keyed, on-demand asset (re-run a historical window), and
# a partitioned asset cannot execute partition-less inside this job.
daily_refresh_job = define_asset_job(
    "daily_world_bank_refresh",
    selection=AssetSelection.assets(
        "fetch_world_bank",
        "fetch_reference",
        "reference_countries",
        "silver_layer",
        "gold_layer",
    ),
    executor_def=single_writer_executor,
)

daily_schedule = ScheduleDefinition(
    name="daily_6am_utc",
    cron_schedule="0 6 * * *",
    target=daily_refresh_job,
    description="Refresh indicators daily at 06:00 UTC (charter req 4).",
)
