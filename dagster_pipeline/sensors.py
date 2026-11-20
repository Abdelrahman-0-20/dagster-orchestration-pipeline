"""Filesystem sensor over data/incoming (charter req 4).

Yields one RunRequest per new file, cursor-deduped. INCOMING_DIR overrides
the location so tests evaluate the sensor offline.
"""
from __future__ import annotations

import os
from pathlib import Path

from dagster import RunRequest, SkipReason, sensor

from .schedules import daily_refresh_job


def _incoming_dir() -> Path:
    env = os.environ.get("INCOMING_DIR")
    if env:
        return Path(env)
    return Path(__file__).resolve().parents[1] / "data" / "incoming"


@sensor(name="incoming_files", job=daily_refresh_job, minimum_interval_seconds=300)
def incoming_files_sensor(context):
    """One RunRequest per new file in data/incoming, deduped via cursor."""
    incoming = _incoming_dir()
    if not incoming.is_dir():
        return SkipReason("data/incoming does not exist yet")
    seen = set(filter(None, (context.cursor or "").split(",")))
    names = sorted(p.name for p in incoming.iterdir() if p.is_file())
    new = [n for n in names if n not in seen]
    if not new:
        return SkipReason("no new incoming files")
    context.update_cursor(",".join(sorted(seen | set(new))))
    for name in new:
        yield RunRequest(run_key=name)
