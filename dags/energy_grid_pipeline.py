"""
### Energy grid pipeline

Hourly European electricity data (Open Power System Data) through a quality-gated ELT flow:

`extract` -> `validate_schema` -> `transform_hourly` (Spark) -> `quality_gate`
-> `build_daily_mart` (Spark) -> `load_postgres` (upsert) -> `publish_report` (SQL -> Markdown/CSV)

Every task is safe to re-run: the download is skipped when complete and written atomically,
Parquet outputs are overwritten, and the Postgres load upserts on (country, date).
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

from pipeline import config

default_args = {"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=2)}


@dag(
    dag_id="energy_grid_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["energy", "spark", "elt"],
    doc_md=__doc__,
)
def energy_grid_pipeline():
    @task
    def extract() -> dict:
        from pipeline.extract import download_raw

        return download_raw()

    @task
    def validate_schema(raw: dict) -> str:
        from pathlib import Path

        from pipeline.extract import missing_columns

        missing = missing_columns(Path(raw["path"]))
        if missing:
            # A schema change upstream needs a human; retrying won't fix it.
            raise AirflowFailException(f"Source schema changed, missing columns: {missing}")
        return raw["path"]

    @task
    def transform_hourly(raw_path: str) -> int:
        from pipeline.transform import clean_hourly, get_spark, read_raw, to_long, write_hourly

        spark = get_spark("energy-transform-hourly")
        try:
            hourly = clean_hourly(to_long(read_raw(spark, raw_path)))
            write_hourly(hourly, str(config.CURATED_DIR))
            return spark.read.parquet(str(config.CURATED_DIR)).count()
        finally:
            spark.stop()

    @task
    def quality_gate(hourly_rows: int, ds=None) -> str:
        from pipeline.quality import decide_action, run_checks, write_results
        from pipeline.transform import get_spark

        spark = get_spark("energy-quality-gate")
        try:
            hourly = spark.read.parquet(str(config.CURATED_DIR))
            results = run_checks(hourly, list(config.COUNTRY_COLUMNS))
        finally:
            spark.stop()
        action = decide_action(results)
        write_results(results, action, config.QUALITY_DIR / f"quality_{ds}.json")
        for r in results:
            print(f"[quality] {r.name}: {'PASS' if r.passed else 'FAIL'} "
                  f"(observed={r.observed:.4g}, threshold={r.threshold:.4g}) {r.detail}")
        if action == "fail":
            raise AirflowFailException("Quality gate failed; see quality report. Nothing was loaded.")
        return action

    @task
    def build_daily_mart(gate: str) -> int:
        from pipeline.transform import daily_energy, get_spark, write_daily

        spark = get_spark("energy-daily-mart")
        try:
            daily = daily_energy(spark.read.parquet(str(config.CURATED_DIR)))
            write_daily(daily, str(config.MART_DIR))
            return spark.read.parquet(str(config.MART_DIR)).count()
        finally:
            spark.stop()

    @task
    def load_postgres(mart_rows: int) -> int:
        from pipeline.load import load_daily

        return load_daily(str(config.MART_DIR), config.ENERGY_DB_URI)

    @task
    def publish_report(loaded_rows: int, ds=None) -> str:
        from pipeline.report import build_report

        return str(build_report(config.ENERGY_DB_URI, config.SQL_DIR, config.REPORT_DIR, ds,
                                quality_path=config.QUALITY_DIR / f"quality_{ds}.json"))

    raw = extract()
    path = validate_schema(raw)
    rows = transform_hourly(path)
    gate = quality_gate(rows)
    mart = build_daily_mart(gate)
    loaded = load_postgres(mart)
    publish_report(loaded)


energy_grid_pipeline()
