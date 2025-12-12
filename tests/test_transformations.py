import os
import shutil
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch

from src.config import config
from src.sources.base import RawFetchResult
from src.sources.worldbank import WorldBankSource
from src.storage.sqlite_storage import SQLiteStorage
from src.transform.environment import transform_environment
from src.transform.macro import transform_macro


class TransformationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_sqlite = config.SQLITE_FILE
        self.temp_dir = tempfile.mkdtemp(prefix="ecopulse_sqlite_transform_")
        config.SQLITE_FILE = os.path.join(self.temp_dir, "ecopulse.sqlite")
        self.sqlite = SQLiteStorage()

    def tearDown(self) -> None:
        self.sqlite.close()
        config.SQLITE_FILE = self.original_sqlite
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_environment_transform_handles_hyphenated_sources(self) -> None:
        timestamp = "2024-01-01T00:00:00Z"
        weather_result = RawFetchResult(
            source="open-meteo-weather-ams",
            url="",
            params={},
            status_code=200,
            ok=True,
            error=None,
            duration_ms=15,
            payload_json={
                "hourly": {
                    "time": [timestamp],
                    "temperature_2m": [6.5],
                    "wind_speed_10m": [12.0],
                    "precipitation": [0.1],
                }
            },
            payload_text=None,
            fetched_at_utc=datetime.utcnow().isoformat(),
        )

        air_result = RawFetchResult(
            source="open-meteo-air-ams",
            url="",
            params={},
            status_code=200,
            ok=True,
            error=None,
            duration_ms=20,
            payload_json={
                "hourly": {
                    "time": [timestamp],
                    "pm2_5": [3.2],
                    "pm10": [4.1],
                    "european_aqi": [51],
                    "us_aqi": [48],
                }
            },
            payload_text=None,
            fetched_at_utc=datetime.utcnow().isoformat(),
        )

        transform_environment([weather_result, air_result], self.sqlite)
        rows = self.sqlite.latest_env_rows("ams")

        self.assertEqual(len(rows), 1, "Expected a combined environment row for Amsterdam")
        stored = rows[0]
        self.assertEqual(
            stored,
            (
                timestamp,
                6.5,
                12.0,
                0.1,
                3.2,
                4.1,
                51,
                48,
            ),
        )

    def test_macro_transform_stores_all_regions(self) -> None:
        indicator = "FP.CPI.TOTL.ZG"
        payload = [
            {"page": 1, "pages": 1, "per_page": 5000},
            [
                {"countryiso3code": "NLD", "date": "2023", "value": 4.5},
                {"countryiso3code": "EUU", "date": "2023", "value": 5.5},
                {"countryiso3code": "USA", "date": "2023", "value": 3.2},
                {"countryiso3code": "WLD", "date": "2023", "value": 2.1},
            ],
        ]
        macro_result = RawFetchResult(
            source=f"worldbank-{indicator}",
            url="",
            params={},
            status_code=200,
            ok=True,
            error=None,
            duration_ms=10,
            payload_json=payload,
            payload_text=None,
            fetched_at_utc=datetime.utcnow().isoformat(),
        )

        transform_macro([macro_result], self.sqlite)
        stored = self.sqlite.macro_latest(indicator)
        values_by_region = {region: value for region, _, value in stored}

        self.assertEqual(len(values_by_region), 4)
        self.assertEqual(values_by_region["NLD"], 4.5)
        self.assertEqual(values_by_region["EUU"], 5.5)
        self.assertEqual(values_by_region["USA"], 3.2)
        self.assertEqual(values_by_region["WLD"], 2.1)


class WorldBankSourceTests(unittest.TestCase):
    def test_fetch_indicator_requests_full_page(self) -> None:
        captured_params = {}

        def fake_do_request(url, params):  # type: ignore[override]
            captured_params.update(params)

            class FakeResponse:
                ok = True
                status_code = 200
                text = "{}"

                def json(self):
                    return [{"page": 1, "pages": 1, "per_page": params.get("per_page")}, []]

            return FakeResponse(), None, 5

        source = WorldBankSource()
        with patch("src.sources.worldbank._do_request", side_effect=fake_do_request):
            result = source._fetch_indicator("FP.CPI.TOTL.ZG")

        self.assertIn("per_page", captured_params)
        self.assertGreaterEqual(captured_params["per_page"], 1000)
        self.assertEqual(result.params["per_page"], captured_params["per_page"])


if __name__ == "__main__":
    unittest.main()
