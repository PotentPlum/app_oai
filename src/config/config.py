import os
from dataclasses import dataclass
from typing import List

DB_NAME = "ecopulse"
SQLITE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "..", "ecopulse.sqlite")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

@dataclass
class Location:
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
    "FP.CPI.TOTL.ZG": "Inflation (annual %)",
    "SL.UEM.TOTL.ZS": "Unemployment (annual %)",
    "NY.GDP.MKTP.KD.ZG": "GDP growth (annual %)",
    "EN.ATM.CO2E.PC": "CO2 emissions (metric tons per capita)",
}

WB_REGIONS = {
    "NLD": "Netherlands",
    "EUU": "European Union",
    "USA": "United States",
    "WLD": "World",
}

USER_AGENT = "EcoPulseDashboard/1.0"
REQUEST_TIMEOUT = 10
MAX_RETRIES = 2
