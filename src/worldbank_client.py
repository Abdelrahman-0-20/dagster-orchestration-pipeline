"""World Bank API v2 client (no API key).

Pagination across pages, retry with exponential backoff, and an injectable
``fetch`` function so tests can serve recorded fixtures from
``tests/fixtures/`` with zero network access (charter req 2).
"""

from __future__ import annotations

import json
import time
import urllib.request
from typing import Callable, Dict, Iterator, List, Optional

DEFAULT_BASE_URL = "https://api.worldbank.org/v2"


class WorldBankAPIError(Exception):
    """Raised when the World Bank API cannot be fetched or parsed."""


def default_fetch(url: str, timeout: float = 30.0) -> str:
    """Real HTTP fetch used in production; tests never call this."""
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read().decode("utf-8")


class WorldBankClient:
    """Client for api.worldbank.org/v2 with injectable transport.

    Parameters
    ----------
    base_url:
        API root, e.g. ``https://api.worldbank.org/v2``.
    fetch:
        Callable ``url -> str`` returning a JSON body. Injected in tests to
        serve recorded fixture pages (zero-network).
    max_retries:
        Number of retries *after* the first attempt (total attempts =
        ``max_retries + 1``) on any fetch/parse failure.
    backoff_seconds:
        Initial backoff delay; doubles after each failed attempt.
    sleep:
        Injectable sleep so tests assert backoff without waiting.
    """

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        fetch: Optional[Callable[[str], str]] = None,
        max_retries: int = 3,
        backoff_seconds: float = 0.5,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.fetch = fetch if fetch is not None else default_fetch
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.sleep = sleep

    # -- request -----------------------------------------------------------
    def page_url(self, indicator: str, page: int, per_page: int) -> str:
        # Open data: no API key anywhere in the URL (charter req 2).
        return (
            f"{self.base_url}/country/all/indicator/{indicator}"
            f"?format=json&page={page}&per_page={per_page}"
        )

    def _get_json(self, url: str):
        delay = self.backoff_seconds
        last_error: Optional[BaseException] = None
        for attempt in range(self.max_retries + 1):
            try:
                body = self.fetch(url)
                return json.loads(body)
            except Exception as exc:  # noqa: BLE001 — retry any transport/parse error
                last_error = exc
                if attempt >= self.max_retries:
                    break
                self.sleep(delay)
                delay *= 2
        raise WorldBankAPIError(
            f"World Bank request failed after {self.max_retries + 1} "
            f"attempt(s): {url}"
        ) from last_error

    # -- parsing -----------------------------------------------------------
    @staticmethod
    def parse_page(payload) -> Dict:
        """Parse a v2 response ``[meta, [rows...]]`` into a page dict."""
        if (
            not isinstance(payload, list)
            or len(payload) != 2
            or not isinstance(payload[0], dict)
            or not isinstance(payload[1], list)
        ):
            raise WorldBankAPIError(
                "unexpected World Bank payload shape: expected [meta, rows]"
            )
        meta, rows = payload[0], payload[1]
        for key in ("page", "pages", "per_page", "total"):
            if key not in meta:
                raise WorldBankAPIError(f"page metadata missing key: {key}")
        return {
            "page": int(meta["page"]),
            "pages": int(meta["pages"]),
            "per_page": int(meta["per_page"]),
            "total": int(meta["total"]),
            "rows": rows,
        }

    def get_page(self, indicator: str, page: int, per_page: int = 100) -> Dict:
        url = self.page_url(indicator, page, per_page)
        return self.parse_page(self._get_json(url))

    # -- pagination --------------------------------------------------------
    def iter_rows(self, indicator: str, per_page: int = 100) -> Iterator[Dict]:
        """Yield every observation row, walking pages 1..pages."""
        page = 1
        while True:
            data = self.get_page(indicator, page, per_page)
            yield from data["rows"]
            if page >= data["pages"]:
                return
            page += 1

    def fetch_all(self, indicator: str, per_page: int = 100) -> List[Dict]:
        """Materialize all pages for one indicator into a list of rows."""
        return list(self.iter_rows(indicator, per_page))
