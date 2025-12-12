from datetime import datetime, timezone
from typing import List
from bs4 import BeautifulSoup

from src.config import config
from src.sources.base import ScrapeResult, _do_request


class WikipediaScraper:
    name = "wikipedia"

    def scrape_all(self) -> List[ScrapeResult]:
        results: List[ScrapeResult] = []
        for loc in config.LOCATIONS:
            results.append(self.scrape(loc.wikipedia_url))
        return results

    def scrape(self, url: str) -> ScrapeResult:
        now = datetime.now(timezone.utc).isoformat()
        resp, error, duration_ms = _do_request(url, {})  # duration used in RawFetch; here unused
        html = resp.text if resp and resp.ok else None
        parsed = None
        err = error or (None if resp and resp.ok else (resp.text if resp else None))
        if html:
            try:
                soup = BeautifulSoup(html, "html.parser")
                title = soup.find("h1").get_text(strip=True) if soup.find("h1") else ""
                para = soup.find("p")
                summary = para.get_text(strip=True) if para else ""
                parsed = {"title": title, "summary": summary}
            except Exception as exc:  # noqa: BLE001
                err = str(exc)
        return ScrapeResult(
            url=url,
            ok=err is None,
            error=err,
            html=html,
            parsed=parsed,
            fetched_at_utc=now,
        )
