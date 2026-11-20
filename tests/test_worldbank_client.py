"""World Bank client tests — hermetic, offline, fixture-injected."""

from __future__ import annotations

import json

import pytest

from src.worldbank_client import WorldBankAPIError, WorldBankClient

from conftest import load_json_fixture

FIXTURE_PAGES = ["wb_page1.json", "wb_page2.json"]


def _payload_with_pages(name: str, pages: int) -> str:
    """Fixture payload with meta['pages'] overridden for pagination tests."""
    payload = load_json_fixture(name)
    payload[0]["pages"] = pages
    return json.dumps(payload)


def test_parse_fixture_page1(wb_page1):
    """Fixture parsing: meta + rows come straight from the recorded response."""
    page = WorldBankClient.parse_page(wb_page1)
    assert page["page"] == 1
    assert page["pages"] == 8778
    assert page["total"] == 17556
    assert len(page["rows"]) == 2
    row = page["rows"][0]
    assert row["indicator"]["id"] == "NY.GDP.MKTP.CD"
    assert row["countryiso3code"] == "AFE"
    assert isinstance(row["value"], (int, float))


def test_pagination_walks_all_pages_offline():
    """Pagination: client walks page=1..pages through the injected fetch."""
    requested = []

    def fetch(url: str) -> str:
        requested.append(url)
        if "page=1" in url:
            return _payload_with_pages("wb_page1.json", pages=2)
        if "page=2" in url:
            return _payload_with_pages("wb_page2.json", pages=2)
        raise AssertionError(f"unexpected page requested: {url}")

    client = WorldBankClient(fetch=fetch, sleep=lambda s: None)
    rows = client.fetch_all("NY.GDP.MKTP.CD", per_page=2)

    assert len(requested) == 2
    assert requested[0].endswith("page=1&per_page=2")
    assert requested[1].endswith("page=2&per_page=2")
    assert len(rows) == 4  # 2 fixture pages x 2 rows
    assert [r["date"] for r in rows] == ["2025", "2024", "2023", "2022"]


def test_urls_target_world_bank_and_carry_no_api_key():
    client = WorldBankClient(fetch=lambda url: json.dumps([{"page": 1, "pages": 1, "per_page": 100, "total": 0}, []]))
    url = client.page_url("SP.POP.TOTL", page=1, per_page=100)
    assert url.startswith("https://api.worldbank.org/v2/")
    assert "format=json" in url
    assert "key" not in url.lower()


def test_retry_with_exponential_backoff_then_success():
    """Retry: transient failures back off (0.5, 1.0) and then succeed."""
    attempts = {"n": 0}
    sleeps = []

    def fetch(url: str) -> str:
        attempts["n"] += 1
        if attempts["n"] <= 2:
            raise ConnectionError("boom")
        return _payload_with_pages("wb_page1.json", pages=1)

    client = WorldBankClient(
        fetch=fetch, max_retries=3, backoff_seconds=0.5, sleep=sleeps.append
    )
    page = client.get_page("NY.GDP.MKTP.CD", page=1, per_page=2)

    assert attempts["n"] == 3
    assert sleeps == [0.5, 1.0]
    assert page["page"] == 1


def test_retry_exhausted_raises_named_error():
    def fetch(url: str) -> str:
        raise ConnectionError("always down")

    client = WorldBankClient(fetch=fetch, max_retries=2, sleep=lambda s: None)
    with pytest.raises(WorldBankAPIError) as excinfo:
        client.get_page("SP.POP.TOTL", page=1)
    assert "api.worldbank.org" in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, ConnectionError)


def test_malformed_payload_raises_named_error():
    client = WorldBankClient(fetch=lambda url: json.dumps({"nope": True}))
    with pytest.raises(WorldBankAPIError):
        client.get_page("SP.POP.TOTL", page=1)
