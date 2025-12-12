"""Lightweight wrapper for optional MongoDB logging."""

from typing import Optional
import logging
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError
from src.config import config
from src.sources.base import RawFetchResult, ScrapeResult

logger = logging.getLogger(__name__)


class MongoStorage:
    """Log raw fetches and scrapes for later inspection.

    The MongoDB dependency is optional; callers can check ``available`` before
    attempting to persist diagnostics. This keeps the rest of the application
    decoupled from Mongo while still enabling deep debugging when it is running
    (e.g., via Docker Compose).
    """

    def __init__(self) -> None:
        self.client: Optional[MongoClient] = None
        self._connect()

    def _connect(self) -> None:
        try:
            self.client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=2000)
            self.client.server_info()
            logger.info("Connected to MongoDB at %s", config.MONGO_URI)
        except PyMongoError as exc:  # noqa: BLE001
            logger.warning("MongoDB unavailable: %s", exc)
            self.client = None

    @property
    def available(self) -> bool:
        """Expose whether MongoDB is reachable (useful for UI status displays)."""

        return self.client is not None

    def _col(self, name: str) -> Collection:
        assert self.client
        return self.client[config.DB_NAME][name]

    def log_fetch(self, result: RawFetchResult) -> None:
        """Persist an API request/response to MongoDB for debugging."""

        if not self.client:
            return
        doc = {
            "source": result.source,
            "url": result.url,
            "params": result.params,
            "status_code": result.status_code,
            "ok": result.ok,
            "error": result.error,
            "duration_ms": result.duration_ms,
            "payload_json": result.payload_json,
            "payload_text": result.payload_text,
            "fetched_at_utc": result.fetched_at_utc,
        }
        self._col("raw_fetches").insert_one(doc)

    def log_scrape(self, result: ScrapeResult) -> None:
        """Persist a scraped HTML page and parsed metadata for auditing."""

        if not self.client:
            return
        doc = {
            "url": result.url,
            "ok": result.ok,
            "error": result.error,
            "html": result.html,
            "parsed": result.parsed,
            "fetched_at_utc": result.fetched_at_utc,
        }
        self._col("scraped_pages").insert_one(doc)
