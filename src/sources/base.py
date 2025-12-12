"""Shared plumbing for data sources and scrapers.

The goal of this module is to centralize request/response handling and the
lightweight data structures used throughout the application. This keeps
individual sources small and debuggable while ensuring consistent logging and
retry behavior across the board.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional
import logging
import time
import requests
from requests import Response
from src.config import config

logger = logging.getLogger(__name__)


@dataclass
class RawFetchResult:
    """Normalized representation of an HTTP API response.

    Fields are intentionally verbose to simplify debugging: every fetch caller
    includes the URL, query parameters, timing, and both parsed and raw payload
    for inspection or replaying requests.
    """

    source: str
    url: str
    params: Dict[str, Any]
    status_code: Optional[int]
    ok: bool
    error: Optional[str]
    duration_ms: int
    payload_json: Optional[Any]
    payload_text: Optional[str]
    fetched_at_utc: str


@dataclass
class ScrapeResult:
    """Normalized representation of an HTML scrape.

    The separation between ``html`` and ``parsed`` allows us to debug parsing
    errors separately from request failures and to save the raw page to MongoDB
    when needed.
    """

    url: str
    ok: bool
    error: Optional[str]
    html: Optional[str]
    parsed: Optional[Dict[str, Any]]
    fetched_at_utc: str


class DataSource:
    """Interface implemented by every HTTP data source."""

    name: str

    def fetch(self) -> list[RawFetchResult]:
        """Retrieve payloads and return normalized results."""

        raise NotImplementedError


def _do_request(url: str, params: Dict[str, Any]) -> tuple[Optional[Response], Optional[str], int]:
    """Perform a GET request with retries and basic diagnostics.

    The function returns the ``requests.Response`` (if any), an error message
    (``None`` on success), and the total duration in milliseconds. The
    triple-return shape is convenient for logging raw results to MongoDB while
    keeping transformation code decoupled from network concerns.
    """

    headers = {"User-Agent": config.USER_AGENT}
    start = time.time()
    resp: Optional[Response] = None
    error = None
    for attempt in range(config.MAX_RETRIES + 1):
        try:
            logger.debug("GET %s params=%s attempt=%s", url, params, attempt + 1)
            resp = requests.get(url, params=params, headers=headers, timeout=config.REQUEST_TIMEOUT)
            logger.debug("Response %s in %sms", resp.status_code if resp else "?", int((time.time() - start) * 1000))
            return resp, None, int((time.time() - start) * 1000)
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
            logger.warning("Request error (attempt %s/%s): %s", attempt + 1, config.MAX_RETRIES + 1, error)
            time.sleep(1 + attempt)
    duration_ms = int((time.time() - start) * 1000)
    logger.error("Failed GET %s after %sms: %s", url, duration_ms, error)
    return resp, error, duration_ms
