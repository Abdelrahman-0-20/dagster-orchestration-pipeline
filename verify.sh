#!/usr/bin/env bash
# M1a verify gate — bare commands, no masking pipes; last segment owns exit code.
set -euo pipefail
cd "$(dirname "$0")"

PY=".venv/bin/python"
if [ ! -x "$PY" ]; then
  PY="python3"
fi

"$PY" -m pytest -q
"$PY" -c "import yaml; yaml.safe_load(open('config.yaml'))"
"$PY" -m compileall -q src dagster_pipeline
