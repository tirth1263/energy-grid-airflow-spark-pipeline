"""Central configuration: paths, source URL, and the country -> column mapping."""
from __future__ import annotations

import os
from pathlib import Path

DATA_DIR = Path(os.environ.get("ENERGY_DATA_DIR", "/opt/airflow/data"))
SQL_DIR = Path(os.environ.get("ENERGY_SQL_DIR", "/opt/airflow/sql"))

# Open Power System Data, "Time series" package (2020-10-06), hourly, single index.
# https://data.open-power-system-data.org/time_series/
RAW_URL = (
    "https://data.open-power-system-data.org/time_series/2020-10-06/"
    "time_series_60min_singleindex.csv"
)
RAW_FILE = DATA_DIR / "raw" / "opsd_time_series_60min.csv"
RAW_MIN_BYTES = 100_000_000  # the full file is ~130 MB; anything far smaller is a bad download

CURATED_DIR = DATA_DIR / "curated" / "hourly"   # long format, partitioned by country/year
MART_DIR = DATA_DIR / "marts" / "daily"         # one row per country per day
QUALITY_DIR = DATA_DIR / "quality"
REPORT_DIR = DATA_DIR / "reports"

ENERGY_DB_URI = os.environ.get(
    "ENERGY_DB_URI", "postgresql+psycopg2://airflow:airflow@postgres:5432/energy"
)

# Country-level series used by the pipeline (values are hourly averages in MW).
COUNTRY_COLUMNS: dict[str, dict[str, str]] = {
    "DE": {
        "load": "DE_load_actual_entsoe_transparency",
        "solar": "DE_solar_generation_actual",
        "wind": "DE_wind_generation_actual",
    },
    "ES": {
        "load": "ES_load_actual_entsoe_transparency",
        "solar": "ES_solar_generation_actual",
        "wind": "ES_wind_onshore_generation_actual",
    },
    "FR": {
        "load": "FR_load_actual_entsoe_transparency",
        "solar": "FR_solar_generation_actual",
        "wind": "FR_wind_onshore_generation_actual",
    },
    "GB": {
        "load": "GB_GBN_load_actual_entsoe_transparency",
        "solar": "GB_GBN_solar_generation_actual",
        "wind": "GB_GBN_wind_generation_actual",
    },
    "IT": {
        "load": "IT_load_actual_entsoe_transparency",
        "solar": "IT_solar_generation_actual",
        "wind": "IT_wind_onshore_generation_actual",
    },
}

METRICS = ("load", "solar", "wind")
