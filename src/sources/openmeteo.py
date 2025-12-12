from datetime import datetime, timezone
from typing import List

from src.config import config
from src.sources.base import DataSource, RawFetchResult, _do_request


class OpenMeteoSource(DataSource):
    name = "open-meteo"

    def fetch(self) -> List[RawFetchResult]:
        results: List[RawFetchResult] = []
        for loc in config.LOCATIONS:
            results.extend(self._fetch_for_location(loc.key, loc.lat, loc.lon))
        return results

    def _fetch_for_location(self, location_key: str, lat: float, lon: float) -> List[RawFetchResult]:
        now = datetime.now(timezone.utc).isoformat()
        outputs: List[RawFetchResult] = []

        weather_url = "https://api.open-meteo.com/v1/forecast"
        weather_params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m,wind_speed_10m,precipitation",
            "timezone": "UTC",
        }
        resp, error, duration_ms = _do_request(weather_url, weather_params)
        payload_json = resp.json() if resp and resp.ok else None
        outputs.append(
            RawFetchResult(
                source=f"{self.name}-weather-{location_key}",
                url=weather_url,
                params=weather_params,
                status_code=resp.status_code if resp else None,
                ok=bool(resp and resp.ok),
                error=error or (None if resp and resp.ok else (resp.text if resp else None)),
                duration_ms=duration_ms,
                payload_json=payload_json,
                payload_text=resp.text if resp else None,
                fetched_at_utc=now,
            )
        )

        air_url = "https://air-quality-api.open-meteo.com/v1/air-quality"
        air_params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": "pm2_5,pm10,european_aqi,us_aqi",
            "timezone": "UTC",
        }
        resp2, error2, duration_ms2 = _do_request(air_url, air_params)
        payload_json2 = resp2.json() if resp2 and resp2.ok else None
        outputs.append(
            RawFetchResult(
                source=f"{self.name}-air-{location_key}",
                url=air_url,
                params=air_params,
                status_code=resp2.status_code if resp2 else None,
                ok=bool(resp2 and resp2.ok),
                error=error2 or (None if resp2 and resp2.ok else (resp2.text if resp2 else None)),
                duration_ms=duration_ms2,
                payload_json=payload_json2,
                payload_text=resp2.text if resp2 else None,
                fetched_at_utc=now,
            )
        )
        return outputs
