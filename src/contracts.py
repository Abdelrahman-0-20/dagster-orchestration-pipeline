"""Data contracts: required fields per source, enforced loudly (charter req 5).

M1a provides the contract basics — presence and type checks with named
errors — consumed at the raw -> silver boundary from M1b onward.
"""

from __future__ import annotations

from typing import Dict, Iterable, Mapping, Sequence


class ContractViolation(Exception):
    """Named, loud failure of a data-contract check (never a silent drop)."""

    def __init__(self, source: str, field: str, message: str) -> None:
        self.source = source
        self.field = field
        super().__init__(f"[contract:{source}] field '{field}': {message}")


# Defaults; config.yaml `contracts:` may override per source.
DEFAULT_REQUIRED_FIELDS: Dict[str, Sequence[str]] = {
    "world_bank": ("indicator", "country", "countryiso3code", "date", "value"),
    "iso3166": ("name", "alpha-3", "region"),
}


def required_fields(source: str, config: Mapping | None = None) -> Sequence[str]:
    """Resolve the required-field list for a source, config taking precedence."""
    if config is not None:
        contracts = config.get("contracts") or {}
        spec = contracts.get(source)
        if spec and spec.get("required_fields"):
            return tuple(spec["required_fields"])
    if source not in DEFAULT_REQUIRED_FIELDS:
        raise ContractViolation(source, "*", "unknown contract source")
    return DEFAULT_REQUIRED_FIELDS[source]


def validate_row(
    source: str,
    row: Mapping,
    config: Mapping | None = None,
) -> Mapping:
    """Validate one raw row against the source contract; return the row.

    Raises :class:`ContractViolation` naming the offending field —
    violations fail loud, they are never silently dropped (charter req 5).
    """
    for field in required_fields(source, config):
        if field not in row:
            raise ContractViolation(source, field, "required field is missing")
        if row[field] is None:
            raise ContractViolation(source, field, "required field is null")
    if source == "world_bank":
        date = row["date"]
        if not isinstance(date, str) or not date.strip():
            raise ContractViolation(
                source, "date", f"expected non-empty str, got {type(date).__name__}"
            )
        value = row["value"]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ContractViolation(
                source,
                "value",
                f"expected numeric, got {type(value).__name__}",
            )
        iso3 = row["countryiso3code"]
        if not isinstance(iso3, str) or len(iso3.strip()) != 3:
            raise ContractViolation(
                source,
                "countryiso3code",
                f"expected 3-letter str, got {iso3!r}",
            )
    return row


def validate_rows(
    source: str,
    rows: Iterable[Mapping],
    config: Mapping | None = None,
) -> int:
    """Validate every row; returns the number of rows validated."""
    count = 0
    for row in rows:
        validate_row(source, row, config)
        count += 1
    return count
