"""CI exists and actually runs the project gate (charter req 8)."""
from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_ci_workflow_runs_the_gate():
    path = ROOT / ".github" / "workflows" / "ci.yml"
    assert path.is_file(), "CI workflow must exist"
    wf = yaml.safe_load(path.read_text(encoding="utf-8"))

    triggers = wf[True] if True in wf else wf["on"]  # YAML parses bare 'on' as True
    assert "push" in triggers and "pull_request" in triggers
    assert "schedule" in triggers  # weekly drift check (charter req 8)

    steps = wf["jobs"]["verify"]["steps"]
    commands = [s.get("run", "") for s in steps]
    assert any("./verify.sh" in c for c in commands), "CI must run ./verify.sh"
