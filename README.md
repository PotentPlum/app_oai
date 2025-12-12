# EcoPulse Dashboard

EcoPulse Dashboard is a local Tkinter application that fetches environmental and macro-economic data from free APIs, enriches it with Wikipedia summaries, stores raw payloads in MongoDB, curates the data in SQLite, and visualizes it in a desktop dashboard.

## Features
- Hourly environment metrics (temperature, wind, precipitation, AQI, PM2.5, PM10) for Amsterdam, Brussels, and New York City.
- Annual macro indicators (inflation, unemployment, GDP growth, CO2 per capita) for Netherlands, EU, USA, and World.
- Wikipedia enrichment (title + first paragraph) for tracked locations.
- Raw data landing in MongoDB; curated facts and dimensions in SQLite with auto-created schema.
- Tkinter UI with tabs: Environment, Macro Compare, and Data Ops (manual fetch, scheduler, CSV export).
- Docker Compose for local MongoDB.

## Architecture Overview
```
/ (project root)
  main.py
  docker-compose.yml
  requirements.txt
  SETUP_LOCAL.md
  src/
    config/config.py           # Constants, tracked locations, indicator metadata
    sources/                   # Data sources
      base.py                  # DataSource base + helpers
      openmeteo.py             # Open-Meteo weather + air quality
      worldbank.py             # World Bank macro indicators
      wikipedia.py             # Wikipedia scraper
    storage/
      mongo_storage.py         # Raw MongoDB landing
      sqlite_storage.py        # Curated SQLite store + schema
    transform/
      environment.py           # Raw -> fact_env_hourly
      macro.py                 # Raw -> fact_macro_annual
    scheduler.py               # Lightweight repeating scheduler helper
    app_service.py             # Orchestration: fetch -> persist -> transform
    ui/dashboard.py            # Tkinter dashboard views
```

Data flow: sources fetch -> raw logged to MongoDB -> transform writes curated tables in SQLite -> UI reads SQLite.

## Adding a New Data Source
1. Create a new file in `src/sources/` that implements `DataSource` from `src/sources/base.py`.
2. Implement `name` and `fetch()` returning a list of `RawFetchResult`.
3. Update transformations to land curated data (create a new module in `src/transform/` if needed).
4. Wire the new source into `AppService.fetch_all()` and persist raw results to MongoDB.
5. Extend the SQLite schema if new curated tables are required.

**Minimal template:**
```python
# src/sources/my_source.py
from datetime import datetime, timezone
from src.sources.base import DataSource, RawFetchResult, _do_request

class MySource(DataSource):
    name = "my-source"
    def fetch(self):
        url = "https://example.com/api"
        params = {"foo": "bar"}
        now = datetime.now(timezone.utc).isoformat()
        resp, error, duration_ms = _do_request(url, params)
        payload = resp.json() if resp and resp.ok else None
        return [RawFetchResult(
            source=self.name,
            url=url,
            params=params,
            status_code=resp.status_code if resp else None,
            ok=bool(resp and resp.ok),
            error=error or (resp.text if resp and not resp.ok else None),
            duration_ms=duration_ms,
            payload_json=payload,
            payload_text=resp.text if resp else None,
            fetched_at_utc=now,
        )]
```

After adding, update `AppService` to call the new source and create matching transform logic.

## Running
See [SETUP_LOCAL.md](SETUP_LOCAL.md) for step-by-step instructions on Windows (Python 3.11+, Docker Desktop required for MongoDB).

## Testing
- Install dependencies (see setup guide) and ensure MongoDB is running if you want the Mongo tests to execute instead of being skipped.
- Run the database sanity checks:
  ```bash
  python -m unittest discover -s tests -p "test_*.py"
  ```
- The suite validates that the SQLite schema is seeded and that inserts/upserts work. MongoDB logging tests are skipped when `pymongo` or MongoDB is unavailable.

## Inspecting Databases Visually
- **MongoDB:** MongoDB Compass offers a GUI for browsing the `raw_fetches` and `scraped_pages` collections.
- **SQLite:** DB Browser for SQLite or SQLiteStudio can open `ecopulse.sqlite`. SQL Server Management Studio (SSMS) can also inspect SQLite files with an appropriate ODBC driver if you already use it.
