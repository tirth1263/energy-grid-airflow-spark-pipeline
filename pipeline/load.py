"""Load the daily mart into Postgres with an idempotent upsert keyed on (country, date)."""
from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine, text

DDL = """
CREATE TABLE IF NOT EXISTS energy_daily (
    country          TEXT        NOT NULL,
    date             DATE        NOT NULL,
    load_mwh         DOUBLE PRECISION,
    solar_mwh        DOUBLE PRECISION,
    wind_mwh         DOUBLE PRECISION,
    renewable_share  DOUBLE PRECISION,
    hours_reported   INTEGER,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (country, date)
);
"""

UPSERT = """
INSERT INTO energy_daily (country, date, load_mwh, solar_mwh, wind_mwh, renewable_share, hours_reported)
SELECT country, date, load_mwh, solar_mwh, wind_mwh, renewable_share, hours_reported
FROM energy_daily_staging
ON CONFLICT (country, date) DO UPDATE SET
    load_mwh        = EXCLUDED.load_mwh,
    solar_mwh       = EXCLUDED.solar_mwh,
    wind_mwh        = EXCLUDED.wind_mwh,
    renewable_share = EXCLUDED.renewable_share,
    hours_reported  = EXCLUDED.hours_reported,
    loaded_at       = now();
"""


def load_daily(mart_path: str, db_uri: str) -> int:
    """Upsert the daily mart. Re-running with the same data leaves the table unchanged."""
    df = pd.read_parquet(mart_path)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    engine = create_engine(db_uri)
    with engine.begin() as conn:  # one transaction: staging + upsert + cleanup commit together
        conn.execute(text(DDL))
        df.to_sql("energy_daily_staging", conn, if_exists="replace", index=False)
        conn.execute(text(UPSERT))
        conn.execute(text("DROP TABLE energy_daily_staging"))
    return len(df)
