"""Transform raw Open-Meteo payloads into curated SQLite rows."""

from typing import List, Dict

from src.config import config
from src.sources.base import RawFetchResult
from src.storage.sqlite_storage import SQLiteStorage


def transform_environment(raw_results: List[RawFetchResult], sqlite_store: SQLiteStorage) -> None:
    """Map raw fetches to the ``fact_env_hourly`` table.

    The Open-Meteo responses return separate weather and air-quality payloads
    per location. We normalize them into a dictionary keyed by timestamp so that
    temperature and AQI readings land on the same row when available, while
    still tolerating missing data.
    """

    weather_data: Dict[str, Dict[str, float]] = {}
    air_data: Dict[str, Dict[str, float]] = {}

    for result in raw_results:
        if not result.ok or not result.payload_json:
            continue
        source_parts = result.source.rsplit("-", 2)
        if len(source_parts) != 3:
            continue
        _, category, location_key = source_parts
        hourly = result.payload_json.get("hourly", {}) if isinstance(result.payload_json, dict) else {}
        times = hourly.get("time", [])
        if category == "weather":
            weather_data[location_key] = {
                ts: {
                    "temp_c": hourly.get("temperature_2m", [None] * len(times))[idx],
                    "wind_kph": hourly.get("wind_speed_10m", [None] * len(times))[idx],
                    "precip_mm": hourly.get("precipitation", [None] * len(times))[idx],
                }
                for idx, ts in enumerate(times)
            }
        elif category == "air":
            air_data[location_key] = {
                ts: {
                    "pm2_5": hourly.get("pm2_5", [None] * len(times))[idx],
                    "pm10": hourly.get("pm10", [None] * len(times))[idx],
                    "european_aqi": hourly.get("european_aqi", [None] * len(times))[idx],
                    "us_aqi": hourly.get("us_aqi", [None] * len(times))[idx],
                }
                for idx, ts in enumerate(times)
            }

    for loc in config.LOCATIONS:
        weather = weather_data.get(loc.key, {})
        air = air_data.get(loc.key, {})
        rows = []
        for ts, wdata in weather.items():
            adata = air.get(ts, {})
            rows.append(
                (
                    loc.key,
                    ts,
                    wdata.get("temp_c"),
                    wdata.get("wind_kph"),
                    wdata.get("precip_mm"),
                    adata.get("pm2_5"),
                    adata.get("pm10"),
                    adata.get("european_aqi"),
                    adata.get("us_aqi"),
                )
            )
        if rows:
            sqlite_store.upsert_env_hourly(rows)
