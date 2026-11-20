"""Contract basics (charter req 5): required fields per source, violations
fail loud with named errors."""

from __future__ import annotations

import csv

import pytest

from src.contracts import ContractViolation, validate_row, validate_rows


def _valid_row(wb_page1):
    return dict(wb_page1[1][0])


def test_valid_world_bank_row_passes(wb_page1):
    row = _valid_row(wb_page1)
    assert validate_row("world_bank", row) is row


def test_contract_from_config_matches_charter(config):
    """config.yaml declares required fields for every source.

    world_bank required fields are STRUCTURE only (these must exist and be
    non-null). value and countryiso3code are deliberately absent: null value
    means "no observation" and "" means "no ISO code" upstream — silver
    counts and drops both (policy), it does not fail on them.
    """
    required = config["contracts"]["world_bank"]["required_fields"]
    for field in ("indicator", "country", "date"):
        assert field in required
    for policy_field in ("value", "countryiso3code"):
        assert policy_field not in required, (
            f"{policy_field} is silver policy, not a structural requirement"
        )
    assert "alpha-3" in config["contracts"]["iso3166"]["required_fields"]


def test_missing_required_field_raises_named_error(wb_page1):
    row = _valid_row(wb_page1)
    del row["date"]
    with pytest.raises(ContractViolation) as excinfo:
        validate_row("world_bank", row)
    assert excinfo.value.field == "date"
    assert excinfo.value.source == "world_bank"
    assert "date" in str(excinfo.value)


def test_wrong_type_raises_named_error(wb_page1):
    row = _valid_row(wb_page1)
    row["value"] = "not-a-number"
    with pytest.raises(ContractViolation) as excinfo:
        validate_row("world_bank", row)
    assert excinfo.value.field == "value"


def test_null_value_is_policy_not_a_violation(wb_page1):
    """value=null means "no observation" upstream (2,841 real rows): the
    contract passes it through; silver counts and drops it."""
    row = _valid_row(wb_page1)
    row["value"] = None
    assert validate_row("world_bank", row) is row


def test_validate_rows_counts_and_fails_loud(wb_page1):
    rows = list(wb_page1[1])
    assert validate_rows("world_bank", rows) == len(rows)
    rows.append({"indicator": {"id": "X"}})  # missing everything else
    with pytest.raises(ContractViolation):
        validate_rows("world_bank", rows)


def test_iso3166_fixture_satisfies_contract(iso3166_csv, config):
    """The SHA-pinned ISO 3166 CSV has the contract's required columns."""
    required = config["contracts"]["iso3166"]["required_fields"]
    with open(iso3166_csv, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for field in required:
            assert field in reader.fieldnames, f"missing column {field}"
        rows = [r for r in reader if r.get("alpha-3") == "AFG"]
    assert rows and rows[0]["region"] == "Asia"
