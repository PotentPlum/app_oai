"""Centralized configuration and metadata for the dashboard.

Keeping constants and simple data structures together makes the application
modular: sources, storage layers, and the UI all read from here rather than
hard-coding values. This also gives debuggers a single place to inspect when
validating which locations, indicators, or scheduler intervals are active.
"""

import os
from dataclasses import dataclass
from typing import List

DB_NAME = "ecopulse"
# Persist SQLite database in the repository root so it is easy to inspect and
# does not depend on the working directory of the process.
SQLITE_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "..",
    "ecopulse.sqlite",
)
# MongoDB URI is overridable for debugging against a remote instance.
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

@dataclass
class Location:
    """Simple data holder for a tracked location.

    The small footprint makes it straightforward to add/remove cities and keep
    the change flowing through the sources, transforms, and UI automatically.
    """

    key: str
    name: str
    lat: float
    lon: float
    wikipedia_url: str

LOCATIONS: List[Location] = [
    Location("ams", "Amsterdam", 52.3676, 4.9041, "https://en.wikipedia.org/wiki/Amsterdam"),
    Location("bru", "Brussels", 50.8503, 4.3517, "https://en.wikipedia.org/wiki/Brussels"),
    Location("nyc", "New York City", 40.7128, -74.0060, "https://en.wikipedia.org/wiki/New_York_City"),
]

WB_INDICATORS = {
    # code -> user-friendly label displayed in the UI
    "FP.CPI.TOTL.ZG": "Inflation (annual %)",
    "SL.UEM.TOTL.ZS": "Unemployment (annual %)",
    "NY.GDP.MKTP.KD.ZG": "GDP growth (annual %)",
    "EN.ATM.CO2E.PC": "CO2 emissions (metric tons per capita)",
}

WB_REGIONS = {
    # ISO code -> long name mapping for macro comparisons
    "NLD": "Netherlands",
    "EUU": "European Union",
    "USA": "United States",
    "WLD": "World",
}

USER_AGENT = "EcoPulseDashboard/1.0"
REQUEST_TIMEOUT = 10
# Retry a couple of times to make transient network failures easier to debug
# without failing the entire fetch run at the first hiccup.
MAX_RETRIES = 2

# Scheduler defaults (in seconds)
ENV_REFRESH_INTERVAL = int(os.getenv("ENV_REFRESH_INTERVAL", 3600))
MACRO_REFRESH_INTERVAL = int(os.getenv("MACRO_REFRESH_INTERVAL", 86400))
WIKI_REFRESH_INTERVAL = int(os.getenv("WIKI_REFRESH_INTERVAL", 604800))
