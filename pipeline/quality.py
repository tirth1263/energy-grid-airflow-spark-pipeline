"""Data-quality checks on the curated hourly table, and the gate that decides whether the run continues."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# A country's load can't plausibly exceed this (Germany peaks around 80 GW).
MAX_PLAUSIBLE_MW = 150_000.0


@dataclass
class CheckResult:
    name: str
    passed: bool
    observed: float
    threshold: float
    detail: str


def run_checks(hourly: DataFrame, expected_countries: list[str]) -> list[CheckResult]:
    results: list[CheckResult] = []

    total = hourly.count()
    results.append(CheckResult("row_count", total > 0, float(total), 1.0, "curated table must not be empty"))

    distinct_keys = hourly.select("ts_utc", "country", "metric").distinct().count()
    dupes = total - distinct_keys
    results.append(CheckResult("duplicate_keys", dupes == 0, float(dupes), 0.0,
                               "rows sharing (ts_utc, country, metric)"))

    gen = hourly.where(F.col("metric").isin("solar", "wind"))
    gen_total = gen.count()
    negative = gen.where(F.col("value_mw") < 0).count()
    neg_rate = negative / gen_total if gen_total else 0.0
    results.append(CheckResult("negative_generation_rate", neg_rate <= 0.001, neg_rate, 0.001,
                               f"{negative} solar/wind readings below 0 MW"))

    spikes = hourly.where(F.col("value_mw") > MAX_PLAUSIBLE_MW).count()
    results.append(CheckResult("implausible_values", spikes == 0, float(spikes), 0.0,
                               f"readings above {MAX_PLAUSIBLE_MW:,.0f} MW"))

    present = {r["country"] for r in hourly.select("country").distinct().collect()}
    missing = sorted(set(expected_countries) - present)
    results.append(CheckResult("country_coverage", not missing, float(len(missing)), 0.0,
                               f"missing countries: {missing or 'none'}"))

    # Completeness: share of expected hourly readings present within each series' own time span.
    span = (
        hourly.groupBy("country", "metric")
        .agg(F.min("ts_utc").alias("first"), F.max("ts_utc").alias("last"), F.count("*").alias("n"))
        .withColumn(
            "expected",
            (F.unix_timestamp("last") - F.unix_timestamp("first")) / 3600 + 1,
        )
        .withColumn("completeness", F.col("n") / F.col("expected"))
    )
    worst = span.orderBy("completeness").first()
    worst_value = float(worst["completeness"]) if worst else 0.0
    worst_series = f"{worst['country']}/{worst['metric']}" if worst else "n/a"
    results.append(CheckResult("min_series_completeness", worst_value >= 0.95, worst_value, 0.95,
                               f"least complete series: {worst_series}"))
    return results


# Structural problems mean the dataset itself is broken: loading it would corrupt the warehouse.
BLOCKING_CHECKS = frozenset({"row_count", "duplicate_keys", "country_coverage"})
# Everything else (spikes, negatives, completeness) is a value problem: keep data flowing,
# but surface it in the quality report so someone investigates.


def decide_action(results: list[CheckResult]) -> str:
    """Turn check results into a pipeline decision: 'fail', 'warn', or 'pass'.

    'fail' stops the DAG before anything reaches the daily mart or Postgres.
    'warn' lets the run continue but records the problem in the quality report.
    'pass' means every check passed.
    """
    failed = {r.name for r in results if not r.passed}
    if failed & BLOCKING_CHECKS:
        return "fail"
    if failed:
        return "warn"
    return "pass"


def write_results(results: list[CheckResult], action: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"action": action, "checks": [asdict(r) for r in results]}
    path.write_text(json.dumps(payload, indent=2))
