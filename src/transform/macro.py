"""Transform World Bank payloads into the fact_macro_annual table."""

from typing import List, Tuple

from src.sources.base import RawFetchResult
from src.storage.sqlite_storage import SQLiteStorage


def transform_macro(raw_results: List[RawFetchResult], sqlite_store: SQLiteStorage) -> None:
    """Flatten indicator payloads into SQLite rows.

    World Bank API responses are arrays where index 1 contains the data list.
    We defensively parse the content to tolerate occasional empty pages and
    convert the year value to an integer for consistent sorting in the UI.
    """

    rows: List[Tuple[str, str, int, float]] = []
    for result in raw_results:
        if not result.ok or not result.payload_json:
            continue
        parts = result.source.split("-")
        if len(parts) < 2:
            continue
        indicator = parts[1]
        payload = result.payload_json
        if not isinstance(payload, list) or len(payload) < 2:
            continue
        data_entries = payload[1]
        for entry in data_entries:
            region = entry.get("countryiso3code")
            year = entry.get("date")
            value = entry.get("value")
            if region and year:
                try:
                    rows.append((region, indicator, int(year), value))
                except ValueError:
                    continue
    if rows:
        sqlite_store.upsert_macro(rows)
