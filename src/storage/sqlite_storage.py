import sqlite3
from contextlib import closing
from typing import Iterable, List, Optional, Tuple

from src.config import config


class SQLiteStorage:
    def __init__(self) -> None:
        self.conn = sqlite3.connect(config.SQLITE_FILE, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self._init_schema()

    def _init_schema(self) -> None:
        with closing(self.conn.cursor()) as cur:
            cur.executescript(
                """
                CREATE TABLE IF NOT EXISTS dim_region(
                    region_code TEXT PRIMARY KEY,
                    region_name TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS dim_indicator(
                    indicator_code TEXT PRIMARY KEY,
                    indicator_name TEXT NOT NULL,
                    unit TEXT,
                    source TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS dim_location(
                    location_key TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    lat REAL NOT NULL,
                    lon REAL NOT NULL,
                    wikipedia_url TEXT,
                    wiki_title TEXT,
                    wiki_summary TEXT
                );
                CREATE TABLE IF NOT EXISTS fact_env_hourly(
                    location_key TEXT NOT NULL,
                    ts_utc TEXT NOT NULL,
                    temp_c REAL,
                    wind_kph REAL,
                    precip_mm REAL,
                    pm2_5 REAL,
                    pm10 REAL,
                    european_aqi REAL,
                    us_aqi REAL,
                    PRIMARY KEY(location_key, ts_utc),
                    FOREIGN KEY(location_key) REFERENCES dim_location(location_key)
                );
                CREATE TABLE IF NOT EXISTS fact_macro_annual(
                    region_code TEXT NOT NULL,
                    indicator_code TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    value REAL,
                    PRIMARY KEY(region_code, indicator_code, year),
                    FOREIGN KEY(region_code) REFERENCES dim_region(region_code),
                    FOREIGN KEY(indicator_code) REFERENCES dim_indicator(indicator_code)
                );
                CREATE TABLE IF NOT EXISTS fetch_run_log(
                    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at_utc TEXT NOT NULL,
                    finished_at_utc TEXT,
                    ok INTEGER NOT NULL,
                    message TEXT
                );
                """
            )
            self.conn.commit()
        self._seed_dimensions()

    def _seed_dimensions(self) -> None:
        with closing(self.conn.cursor()) as cur:
            cur.executemany(
                "INSERT OR IGNORE INTO dim_region(region_code, region_name) VALUES (?, ?)",
                [(code, name) for code, name in config.WB_REGIONS.items()],
            )
            cur.executemany(
                "INSERT OR IGNORE INTO dim_indicator(indicator_code, indicator_name, unit, source) VALUES (?, ?, ?, ?)",
                [
                    (code, name, None, "World Bank")
                    for code, name in config.WB_INDICATORS.items()
                ],
            )
            cur.executemany(
                "INSERT OR IGNORE INTO dim_location(location_key, name, lat, lon, wikipedia_url) VALUES (?, ?, ?, ?, ?)",
                [
                    (loc.key, loc.name, loc.lat, loc.lon, loc.wikipedia_url)
                    for loc in config.LOCATIONS
                ],
            )
            self.conn.commit()

    def update_location_wiki(self, location_key: str, title: Optional[str], summary: Optional[str]) -> None:
        with closing(self.conn.cursor()) as cur:
            cur.execute(
                "UPDATE dim_location SET wiki_title=?, wiki_summary=? WHERE location_key=?",
                (title, summary, location_key),
            )
            self.conn.commit()

    def upsert_env_hourly(self, rows: Iterable[Tuple[str, str, Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], Optional[float]]]) -> None:
        with closing(self.conn.cursor()) as cur:
            cur.executemany(
                """
                INSERT INTO fact_env_hourly(location_key, ts_utc, temp_c, wind_kph, precip_mm, pm2_5, pm10, european_aqi, us_aqi)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(location_key, ts_utc) DO UPDATE SET
                    temp_c=excluded.temp_c,
                    wind_kph=excluded.wind_kph,
                    precip_mm=excluded.precip_mm,
                    pm2_5=excluded.pm2_5,
                    pm10=excluded.pm10,
                    european_aqi=excluded.european_aqi,
                    us_aqi=excluded.us_aqi;
                """,
                list(rows),
            )
            self.conn.commit()

    def upsert_macro(self, rows: Iterable[Tuple[str, str, int, Optional[float]]]) -> None:
        with closing(self.conn.cursor()) as cur:
            cur.executemany(
                """
                INSERT INTO fact_macro_annual(region_code, indicator_code, year, value)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(region_code, indicator_code, year) DO UPDATE SET
                    value=excluded.value;
                """,
                list(rows),
            )
            self.conn.commit()

    def log_run(self, started: str, finished: Optional[str], ok: bool, message: str) -> None:
        with closing(self.conn.cursor()) as cur:
            cur.execute(
                "INSERT INTO fetch_run_log(started_at_utc, finished_at_utc, ok, message) VALUES (?, ?, ?, ?)",
                (started, finished, int(ok), message),
            )
            self.conn.commit()

    def latest_env_rows(self, location_key: str, limit: int = 48) -> List[Tuple]:
        with closing(self.conn.cursor()) as cur:
            cur.execute(
                "SELECT ts_utc, temp_c, wind_kph, precip_mm, pm2_5, pm10, european_aqi, us_aqi FROM fact_env_hourly WHERE location_key=? ORDER BY ts_utc DESC LIMIT ?",
                (location_key, limit),
            )
            return cur.fetchall()

    def latest_env_kpis(self, location_key: str) -> Optional[Tuple]:
        with closing(self.conn.cursor()) as cur:
            cur.execute(
                "SELECT ts_utc, temp_c, wind_kph, precip_mm, pm2_5, pm10, european_aqi, us_aqi FROM fact_env_hourly WHERE location_key=? ORDER BY ts_utc DESC LIMIT 1",
                (location_key,),
            )
            return cur.fetchone()

    def macro_series(self, indicator: str, start_year: int, end_year: int) -> List[Tuple]:
        with closing(self.conn.cursor()) as cur:
            cur.execute(
                """
                SELECT region_code, year, value FROM fact_macro_annual
                WHERE indicator_code=? AND year BETWEEN ? AND ?
                ORDER BY year ASC
                """,
                (indicator, start_year, end_year),
            )
            return cur.fetchall()

    def macro_latest(self, indicator: str) -> List[Tuple]:
        with closing(self.conn.cursor()) as cur:
            cur.execute(
                """
                SELECT region_code, year, value FROM fact_macro_annual
                WHERE indicator_code=?
                ORDER BY year DESC
                """,
                (indicator,),
            )
            return cur.fetchall()

    def close(self) -> None:
        self.conn.close()
