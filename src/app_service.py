import threading
import time
from datetime import datetime, timezone
from typing import Callable, Optional

from src.sources.openmeteo import OpenMeteoSource
from src.sources.worldbank import WorldBankSource
from src.sources.wikipedia import WikipediaScraper
from src.storage.mongo_storage import MongoStorage
from src.storage.sqlite_storage import SQLiteStorage
from src.transform.environment import transform_environment
from src.transform.macro import transform_macro


class AppService:
    def __init__(self) -> None:
        self.mongo = MongoStorage()
        self.sqlite = SQLiteStorage()
        self.openmeteo = OpenMeteoSource()
        self.worldbank = WorldBankSource()
        self.wikipedia = WikipediaScraper()
        self._scheduler_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._status_callback: Optional[Callable[[str], None]] = None

    def set_status_callback(self, callback: Callable[[str], None]) -> None:
        self._status_callback = callback

    def _notify(self, message: str) -> None:
        if self._status_callback:
            self._status_callback(message)

    def fetch_all(self) -> None:
        started = datetime.now(timezone.utc).isoformat()
        self._notify("Running data fetch...")
        ok = True
        try:
            weather_results = self.fetch_environment()
            macro_results = self.fetch_macro()
            self.fetch_wikipedia()
            self._notify("Fetch complete")
            self.sqlite.log_run(started, datetime.now(timezone.utc).isoformat(), True, "ok")
        except Exception as exc:  # noqa: BLE001
            ok = False
            self._notify(f"Fetch failed: {exc}")
            self.sqlite.log_run(started, datetime.now(timezone.utc).isoformat(), False, str(exc))
        return ok

    def fetch_environment(self):
        weather_results = self.openmeteo.fetch()
        for res in weather_results:
            if self.mongo.available:
                self.mongo.log_fetch(res)
        transform_environment(weather_results, self.sqlite)
        return weather_results

    def fetch_macro(self):
        macro_results = self.worldbank.fetch()
        for res in macro_results:
            if self.mongo.available:
                self.mongo.log_fetch(res)
        transform_macro(macro_results, self.sqlite)
        return macro_results

    def fetch_wikipedia(self):
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
        return scrape_results

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
                self.fetch_environment()
                next_env = now + 3600
            if now >= next_macro:
                self.fetch_macro()
                next_macro = now + 86400
            if now >= next_wiki:
                self.fetch_wikipedia()
                next_wiki = now + 604800
            time.sleep(5)

    def stop_scheduler(self) -> None:
        self._stop_event.set()
        self._notify("Scheduler stopped")

    def is_scheduler_running(self) -> bool:
        return bool(self._scheduler_thread and self._scheduler_thread.is_alive())
