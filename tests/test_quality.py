from datetime import datetime

import pytest

from pipeline.quality import CheckResult, decide_action, run_checks


def hourly_df(spark, rows):
    return spark.createDataFrame(rows, "ts_utc timestamp, country string, metric string, value_mw double")


def by_name(results):
    return {r.name: r for r in results}


def test_clean_data_passes_all_checks(spark):
    rows = [(datetime(2020, 6, 1, h), "XX", m, 50.0) for h in range(24) for m in ("load", "solar", "wind")]
    results = by_name(run_checks(hourly_df(spark, rows), ["XX"]))
    assert all(r.passed for r in results.values()), results


def test_detects_negative_spikes_duplicates_and_missing_country(spark):
    rows = [
        (datetime(2020, 6, 1, 0), "XX", "solar", -5.0),
        (datetime(2020, 6, 1, 1), "XX", "solar", 10.0),
        (datetime(2020, 6, 1, 1), "XX", "solar", 10.0),       # duplicate key
        (datetime(2020, 6, 1, 2), "XX", "load", 900_000.0),   # implausible
    ]
    results = by_name(run_checks(hourly_df(spark, rows), ["XX", "YY"]))
    assert not results["negative_generation_rate"].passed
    assert not results["duplicate_keys"].passed
    assert not results["implausible_values"].passed
    assert not results["country_coverage"].passed


def test_detects_incomplete_series(spark):
    rows = [(datetime(2020, 6, 1, h), "XX", "load", 50.0) for h in (0, 1, 23)]  # 3 of 24 hours
    results = by_name(run_checks(hourly_df(spark, rows), ["XX"]))
    assert not results["min_series_completeness"].passed


# --- decide_action: minimal guardrails every policy must satisfy ---

def _result(name, passed):
    return CheckResult(name, passed, 0.0, 0.0, "")


ALL_CHECKS = ["row_count", "duplicate_keys", "negative_generation_rate",
              "implausible_values", "country_coverage", "min_series_completeness"]


def test_all_passing_checks_pass():
    assert decide_action([_result(n, True) for n in ALL_CHECKS]) == "pass"


@pytest.mark.parametrize("blocking", ["row_count", "duplicate_keys"])
def test_empty_or_duplicated_data_never_loads(blocking):
    results = [_result(n, n != blocking) for n in ALL_CHECKS]
    assert decide_action(results) == "fail"


def test_action_is_always_one_of_three_values():
    for failing in ALL_CHECKS:
        results = [_result(n, n != failing) for n in ALL_CHECKS]
        assert decide_action(results) in {"fail", "warn", "pass"}
