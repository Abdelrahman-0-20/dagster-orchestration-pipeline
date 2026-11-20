"""Shared test fixtures — zero-network: recorded World Bank responses from
tests/fixtures/ served through an injected fetch function (charter req 2)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

FIXTURES = ROOT / "tests" / "fixtures"


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return ROOT


@pytest.fixture(scope="session")
def config() -> dict:
    with open(ROOT / "config.yaml", "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


@pytest.fixture(scope="session")
def wb_page1() -> list:
    with open(FIXTURES / "wb_page1.json", "r", encoding="utf-8") as fh:
        return json.load(fh)


@pytest.fixture(scope="session")
def wb_page2() -> list:
    with open(FIXTURES / "wb_page2.json", "r", encoding="utf-8") as fh:
        return json.load(fh)


@pytest.fixture(scope="session")
def iso3166_csv() -> Path:
    path = FIXTURES / "iso3166.csv"
    assert path.is_file(), "iso3166 fixture must exist on disk"
    return path


def load_json_fixture(name: str):
    with open(FIXTURES / name, "r", encoding="utf-8") as fh:
        return json.load(fh)
