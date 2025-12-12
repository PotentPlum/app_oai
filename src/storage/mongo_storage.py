from typing import Optional
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError
from src.config import config
from src.sources.base import RawFetchResult, ScrapeResult


class MongoStorage:
    def __init__(self) -> None:
        self.client: Optional[MongoClient] = None
        self._connect()

    def _connect(self) -> None:
        try:
            self.client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=2000)
            self.client.server_info()
        except PyMongoError:
            self.client = None

    @property
    def available(self) -> bool:
        return self.client is not None

    def _col(self, name: str) -> Collection:
        assert self.client
        return self.client[config.DB_NAME][name]

    def log_fetch(self, result: RawFetchResult) -> None:
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
