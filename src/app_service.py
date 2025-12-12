"""Orchestration layer tying sources, storage, and transforms together."""

import threading
import time
from datetime import datetime, timezone
from typing import Callable, Optional
import logging

from src.sources.openmeteo import OpenMeteoSource
from src.sources.worldbank import WorldBankSource
from src.sources.wikipedia import WikipediaScraper
from src.storage.mongo_storage import MongoStorage
from src.storage.sqlite_storage import SQLiteStorage
from src.transform.environment import transform_environment
from src.transform.macro import transform_macro
from src.config import config

logger = logging.getLogger(__name__)


class AppService:
    """Central coordinator used by the UI and automated scheduler."""

    def __init__(self) -> None:
        self.mongo = MongoStorage()
        self.sqlite = SQLiteStorage()
        self.openmeteo = OpenMeteoSource()
        self.worldbank = WorldBankSource()
        self.wikipedia = WikipediaScraper()
        self._scheduler_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._status_callback: Optional[Callable[[str], None]] = None
        self.env_interval = config.ENV_REFRESH_INTERVAL
        self.macro_interval = config.MACRO_REFRESH_INTERVAL
        self.wiki_interval = config.WIKI_REFRESH_INTERVAL

    def set_status_callback(self, callback: Callable[[str], None]) -> None:
        """Register a UI-friendly callback for status updates."""

        self._status_callback = callback

    def _notify(self, message: str) -> None:
        """Send a message to the UI layer and log it for debugging."""

        logger.info(message)
        if self._status_callback:
            self._status_callback(message)

    def fetch_all(self) -> None:
        """Fetch all sources in sequence and log aggregate success."""

        started = datetime.now(timezone.utc).isoformat()
        self._notify("Running data fetch...")
        ok = True
        try:
            self.fetch_environment()
            self.fetch_macro()
            self.fetch_wikipedia()
            self._notify("Fetch complete")
            self.sqlite.log_run(started, datetime.now(timezone.utc).isoformat(), True, "ok")
        except Exception as exc:  # noqa: BLE001
            ok = False
            self._notify(f"Fetch failed: {exc}")
            self.sqlite.log_run(started, datetime.now(timezone.utc).isoformat(), False, str(exc))
        return ok

    def _log_source_run(self, name: str, started: str, ok: bool, message: str, count: int) -> None:
        """Persist per-source execution metadata for later review."""

        self.sqlite.log_source_run(
            source_name=name,
            started=started,
            finished=datetime.now(timezone.utc).isoformat(),
            ok=ok,
            message=message,
            item_count=count,
        )

    def fetch_environment(self):
        """Fetch and transform weather + air quality data."""

        started = datetime.now(timezone.utc).isoformat()
        self._notify("Fetching environment data...")
        try:
            weather_results = self.openmeteo.fetch()
            for res in weather_results:
                if self.mongo.available:
                    self.mongo.log_fetch(res)
            transform_environment(weather_results, self.sqlite)
            self._log_source_run("environment", started, True, "ok", len(weather_results))
            self._notify(f"Environment updated ({len(weather_results)} payloads)")
            return weather_results
        except Exception as exc:  # noqa: BLE001
            self._log_source_run("environment", started, False, str(exc), 0)
            self._notify(f"Environment failed: {exc}")
            logger.exception("Environment fetch failed")
            raise

    def fetch_macro(self):
        """Fetch and transform macro indicators."""

        started = datetime.now(timezone.utc).isoformat()
        self._notify("Fetching macro data...")
        try:
            macro_results = self.worldbank.fetch()
            for res in macro_results:
                if self.mongo.available:
                    self.mongo.log_fetch(res)
            transform_macro(macro_results, self.sqlite)
            self._log_source_run("macro", started, True, "ok", len(macro_results))
            self._notify(f"Macro updated ({len(macro_results)} payloads)")
            return macro_results
        except Exception as exc:  # noqa: BLE001
            self._log_source_run("macro", started, False, str(exc), 0)
            self._notify(f"Macro failed: {exc}")
            logger.exception("Macro fetch failed")
            raise

    def fetch_wikipedia(self):
        """Fetch and persist Wikipedia enrichments."""

        started = datetime.now(timezone.utc).isoformat()
        self._notify("Refreshing Wikipedia summaries...")
        try:
            scrape_results = self.wikipedia.scrape_all()
            for sres in scrape_results:
                if self.mongo.available:
                    self.mongo.log_scrape(sres)
                if sres.ok and sres.parsed:
                    for loc in self.sqlite.conn.execute(
                        "SELECT location_key, wikipedia_url FROM dim_location WHERE wikipedia_url=?",
                        (sres.url,),
                    ):
                        self.sqlite.update_location_wiki(loc[0], sres.parsed.get("title"), sres.parsed.get("summary"))
            self._log_source_run("wikipedia", started, True, "ok", len(scrape_results))
            self._notify(f"Wikipedia updated ({len(scrape_results)} pages)")
            return scrape_results
        except Exception as exc:  # noqa: BLE001
            self._log_source_run("wikipedia", started, False, str(exc), 0)
            self._notify(f"Wikipedia failed: {exc}")
            logger.exception("Wikipedia fetch failed")
            raise

    def start_scheduler(self) -> None:
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            return
        self._stop_event.clear()
        self._scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self._scheduler_thread.start()
        self._notify("Scheduler started")

    def _run_scheduler(self) -> None:
        next_env = time.time()
        next_macro = time.time()
        next_wiki = time.time()
        while not self._stop_event.is_set():
            now = time.time()
            if now >= next_env:
                self._notify("Scheduler: triggering environment fetch")
                self.fetch_environment()
                next_env = now + self.env_interval
            if now >= next_macro:
                self._notify("Scheduler: triggering macro fetch")
                self.fetch_macro()
                next_macro = now + self.macro_interval
            if now >= next_wiki:
                self._notify("Scheduler: triggering Wikipedia refresh")
                self.fetch_wikipedia()
                next_wiki = now + self.wiki_interval
            time.sleep(5)

    def stop_scheduler(self) -> None:
        self._stop_event.set()
        self._notify("Scheduler stopped")

    def is_scheduler_running(self) -> bool:
        return bool(self._scheduler_thread and self._scheduler_thread.is_alive())

    def update_intervals(self, env_seconds: int, macro_seconds: int, wiki_seconds: int) -> None:
        self.env_interval = env_seconds
        self.macro_interval = macro_seconds
        self.wiki_interval = wiki_seconds
        self._notify(
            f"Intervals updated (env: {env_seconds}s, macro: {macro_seconds}s, wiki: {wiki_seconds}s)"
        )
