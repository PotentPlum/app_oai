import importlib.util
import os
import shutil
import tempfile
import unittest

from dataclasses import dataclass
from datetime import datetime

from src.config import config
from src.storage.sqlite_storage import SQLiteStorage

pymongo_available = importlib.util.find_spec("pymongo") is not None

if pymongo_available:
    from src.storage.mongo_storage import MongoStorage


@dataclass
class DummyFetchResult:
    source: str
    url: str
    params: dict
    status_code: int
    ok: bool
    error: str | None
    duration_ms: int
    payload_json: dict | None
    payload_text: str | None
    fetched_at_utc: str


@dataclass
class DummyScrapeResult:
    url: str
    ok: bool
    error: str | None
    html: str | None
    parsed: dict | None
    fetched_at_utc: str


class SQLiteStorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.orig_sqlite_path = config.SQLITE_FILE
        self.temp_dir = tempfile.mkdtemp(prefix="ecopulse_sqlite_test_")
        config.SQLITE_FILE = os.path.join(self.temp_dir, "ecopulse.sqlite")
        self.sqlite = SQLiteStorage()

    def tearDown(self) -> None:
        self.sqlite.close()
        config.SQLITE_FILE = self.orig_sqlite_path
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_dimensions_seeded(self) -> None:
        with self.sqlite.conn as conn:
            region_count = conn.execute("SELECT COUNT(*) FROM dim_region").fetchone()[0]
            indicator_count = conn.execute("SELECT COUNT(*) FROM dim_indicator").fetchone()[0]
            location_count = conn.execute("SELECT COUNT(*) FROM dim_location").fetchone()[0]
        self.assertGreater(region_count, 0, "Expected dim_region to be pre-seeded")
        self.assertGreater(indicator_count, 0, "Expected dim_indicator to be pre-seeded")
        self.assertGreater(location_count, 0, "Expected dim_location to be pre-seeded")

    def test_upserts_write_and_update(self) -> None:
        env_rows = [
            ("ams", "2024-01-01T00:00:00Z", 5.0, 10.0, 0.2, 3.3, 4.4, 55.0, 60.0),
            ("bru", "2024-01-01T01:00:00Z", 6.0, 11.0, 0.1, 3.0, 4.0, 50.0, 58.0),
        ]
        self.sqlite.upsert_env_hourly(env_rows)
        updated_rows = [
            ("ams", "2024-01-01T00:00:00Z", 7.5, 12.0, 0.3, 2.5, 3.5, 53.0, 59.0)
        ]
        self.sqlite.upsert_env_hourly(updated_rows)

        macro_rows = [("NLD", "FP.CPI.TOTL.ZG", 2023, 4.2)]
        self.sqlite.upsert_macro(macro_rows)
        macro_update = [("NLD", "FP.CPI.TOTL.ZG", 2023, 4.5)]
        self.sqlite.upsert_macro(macro_update)

        with self.sqlite.conn as conn:
            env_after = conn.execute(
                "SELECT temp_c, wind_kph, precip_mm FROM fact_env_hourly WHERE location_key=? AND ts_utc=?",
                ("ams", "2024-01-01T00:00:00Z"),
            ).fetchone()
            macro_after = conn.execute(
                "SELECT value FROM fact_macro_annual WHERE region_code=? AND indicator_code=? AND year=?",
                ("NLD", "FP.CPI.TOTL.ZG", 2023),
            ).fetchone()

        self.assertEqual(env_after, (7.5, 12.0, 0.3))
        self.assertEqual(macro_after[0], 4.5)

    def test_run_log_records_entries(self) -> None:
        started = datetime.utcnow().isoformat()
        finished = datetime.utcnow().isoformat()
        self.sqlite.log_run(started, finished, True, "ok")
        with self.sqlite.conn as conn:
            row = conn.execute(
                "SELECT started_at_utc, finished_at_utc, ok, message FROM fetch_run_log ORDER BY run_id DESC LIMIT 1"
            ).fetchone()
        self.assertEqual(row[0], started)
        self.assertEqual(row[1], finished)
        self.assertEqual(row[2], 1)
        self.assertEqual(row[3], "ok")


@unittest.skipUnless(pymongo_available, "pymongo is not installed; install requirements to test MongoDB")
class MongoStorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_db_name = config.DB_NAME
        config.DB_NAME = "ecopulse_test"
        self.mongo = MongoStorage()

    def tearDown(self) -> None:
        if self.mongo.client:
            self.mongo.client.drop_database(config.DB_NAME)
        config.DB_NAME = self.original_db_name

    def test_mongo_connection_and_logging(self) -> None:
        if not self.mongo.available:
            self.skipTest("MongoDB is not available (start docker compose first)")

        fetch_result = DummyFetchResult(
            source="test-source",
            url="http://example.com",
            params={"foo": "bar"},
            status_code=200,
            ok=True,
            error=None,
            duration_ms=123,
            payload_json={"hello": "world"},
            payload_text="body",
            fetched_at_utc="2024-01-01T00:00:00Z",
        )
        scrape_result = DummyScrapeResult(
            url="http://example.com/page",
            ok=True,
            error=None,
            html="<html></html>",
            parsed={"title": "Example", "summary": "Short"},
            fetched_at_utc="2024-01-01T00:00:05Z",
        )

        self.mongo.log_fetch(fetch_result)
        self.mongo.log_scrape(scrape_result)

        fetch_doc = self.mongo._col("raw_fetches").find_one({"source": "test-source"})
        scrape_doc = self.mongo._col("scraped_pages").find_one({"url": "http://example.com/page"})

        self.assertIsNotNone(fetch_doc, "raw_fetches should contain the inserted document")
        self.assertIsNotNone(scrape_doc, "scraped_pages should contain the inserted document")


if __name__ == "__main__":
    unittest.main()
