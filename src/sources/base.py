from dataclasses import dataclass
from typing import Any, Dict, Optional
import time
import requests
from requests import Response
from src.config import config


@dataclass
class RawFetchResult:
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
    url: str
    ok: bool
    error: Optional[str]
    html: Optional[str]
    parsed: Optional[Dict[str, Any]]
    fetched_at_utc: str


class DataSource:
    name: str

    def fetch(self) -> list[RawFetchResult]:
        raise NotImplementedError


def _do_request(url: str, params: Dict[str, Any]) -> tuple[Optional[Response], Optional[str], int]:
    headers = {"User-Agent": config.USER_AGENT}
    start = time.time()
    resp: Optional[Response] = None
    error = None
    for attempt in range(config.MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=config.REQUEST_TIMEOUT)
            return resp, None, int((time.time() - start) * 1000)
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
            time.sleep(1 + attempt)
    duration_ms = int((time.time() - start) * 1000)
    return resp, error, duration_ms
