"""World Bank macro-economic data source."""

from datetime import datetime, timezone
from typing import List
import logging

from src.config import config
from src.sources.base import DataSource, RawFetchResult, _do_request

logger = logging.getLogger(__name__)


class WorldBankSource(DataSource):
    """Download annual macro indicators for the configured regions."""

    name = "worldbank"

    def fetch(self) -> List[RawFetchResult]:
        """Collect the configured World Bank indicators in one pass."""

        results: List[RawFetchResult] = []
        for indicator in config.WB_INDICATORS.keys():
            logger.info("Fetching World Bank indicator %s", indicator)
            results.append(self._fetch_indicator(indicator))
        return results

    def _fetch_indicator(self, indicator: str) -> RawFetchResult:
        """Fetch a single indicator for all supported regions."""

        now = datetime.now(timezone.utc).isoformat()
        url = f"https://api.worldbank.org/v2/country/NLD;EUU;USA;WLD/indicator/{indicator}"
        params = {"format": "json", "per_page": 5000}
        resp, error, duration_ms = _do_request(url, params)
        payload_json = resp.json() if resp and resp.ok else None
        return RawFetchResult(
            source=f"{self.name}-{indicator}",
            url=url,
            params=params,
            status_code=resp.status_code if resp else None,
            ok=bool(resp and resp.ok),
            error=error or (None if resp and resp.ok else (resp.text if resp else None)),
            duration_ms=duration_ms,
            payload_json=payload_json,
            payload_text=resp.text if resp else None,
            fetched_at_utc=now,
        )
