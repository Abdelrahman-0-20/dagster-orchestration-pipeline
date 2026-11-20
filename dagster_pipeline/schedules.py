"""Daily job + schedule (charter req 4)."""
from dagster import AssetSelection, ScheduleDefinition, define_asset_job

daily_refresh_job = define_asset_job(
    "daily_world_bank_refresh",
    selection=AssetSelection.all(),
)

daily_schedule = ScheduleDefinition(
    name="daily_6am_utc",
    cron_schedule="0 6 * * *",
    target=daily_refresh_job,
    description="Refresh indicators daily at 06:00 UTC (charter req 4).",
)
