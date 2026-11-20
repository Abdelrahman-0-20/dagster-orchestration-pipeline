"""Single entry point: Definitions(assets, jobs, schedules, sensors)."""
from dagster import Definitions

from .assets import (
    fetch_reference,
    fetch_world_bank,
    gold_backfill,
    gold_layer,
    reference_countries,
    silver_layer,
)
from .schedules import daily_refresh_job, daily_schedule
from .sensors import incoming_files_sensor

defs = Definitions(
    assets=[
        fetch_world_bank,
        fetch_reference,
        reference_countries,
        silver_layer,
        gold_layer,
        gold_backfill,
    ],
    jobs=[daily_refresh_job],
    schedules=[daily_schedule],
    sensors=[incoming_files_sensor],
)
