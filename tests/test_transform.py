from datetime import date

import pytest

from pipeline.transform import clean_hourly, daily_energy, to_long

COLS = {"XX": {"load": "XX_load", "solar": "XX_solar", "wind": "XX_wind"}}


def raw_df(spark, rows):
    return spark.createDataFrame(rows, ["utc_timestamp", "XX_load", "XX_solar", "XX_wind"])


def test_to_long_unpivots_and_casts(spark):
    df = raw_df(spark, [("2020-06-01T10:00:00Z", "100.0", "40.0", "10.0")])
    out = to_long(df, COLS).collect()
    assert len(out) == 3
    by_metric = {r["metric"]: r["value_mw"] for r in out}
    assert by_metric == {"load": 100.0, "solar": 40.0, "wind": 10.0}
    assert all(r["country"] == "XX" for r in out)


def test_clean_drops_nulls_and_duplicate_keys(spark):
    df = raw_df(spark, [
        ("2020-06-01T10:00:00Z", "100.0", "", "bad"),   # empty and non-numeric -> null -> dropped
        ("2020-06-01T10:00:00Z", "100.0", "40.0", "10.0"),  # duplicate timestamp
    ])
    out = clean_hourly(to_long(df, COLS))
    assert out.count() == 3  # one row per metric survives
    assert out.select("ts_utc", "country", "metric").distinct().count() == 3
    assert {r["date"] for r in out.collect()} == {date(2020, 6, 1)}


def test_daily_energy_sums_mwh_and_share(spark):
    df = raw_df(spark, [
        ("2020-06-01T10:00:00Z", "100.0", "40.0", "10.0"),
        ("2020-06-01T11:00:00Z", "100.0", "60.0", "10.0"),
    ])
    row = daily_energy(clean_hourly(to_long(df, COLS))).collect()[0]
    assert row["load_mwh"] == pytest.approx(200.0)
    assert row["solar_mwh"] == pytest.approx(100.0)
    assert row["wind_mwh"] == pytest.approx(20.0)
    assert row["renewable_share"] == pytest.approx(0.6)
    assert row["hours_reported"] == 2
