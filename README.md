# Energy Grid Pipeline: Airflow + Spark ELT

A quality-gated ELT pipeline that turns ~130 MB of real hourly European electricity data into an analytics-ready Postgres mart and an automated SQL report. Orchestrated with **Apache Airflow**, transformed with **PySpark**, and runnable end to end with one `docker compose up`.

```mermaid
flowchart LR
    A[extract<br/>idempotent download] --> B[validate_schema]
    B --> C[transform_hourly<br/>PySpark: wide to long, clean, dedupe]
    C --> D{quality_gate<br/>6 checks}
    D -- fail --> X[stop: nothing loaded]
    D -- pass / warn --> E[build_daily_mart<br/>PySpark: daily MWh + renewable share]
    E --> F[load_postgres<br/>upsert on country + date]
    F --> G[publish_report<br/>SQL window functions to Markdown + CSV]
```

## Data

[Open Power System Data, Time series package](https://data.open-power-system-data.org/time_series/) (version 2020-10-06), hourly, 2015-2020. It's built from ENTSO-E transparency data and national grid operators.

The pipeline uses country-level **actual load**, **solar generation** and **wind generation** (hourly average MW) for Germany, Spain, France, Great Britain and Italy. The column mapping lives in [`pipeline/config.py`](pipeline/config.py). Add a country by adding one entry there.

> Open Power System Data. 2020. Data Package Time series. Version 2020-10-06. https://doi.org/10.25832/time_series/2020-10-06

The dataset is a fixed snapshot, so a daily schedule re-processes the same file. The DAG is built to make that safe (see [Idempotency](#idempotency)) and would work unchanged against a source that updates.

## Quick start

Requirements: Docker Desktop (about 4 GB RAM free).

```bash
docker compose up -d --build        # Postgres + Airflow (webserver, scheduler) with Spark and Java
# open http://localhost:8080  (admin / admin), unpause "energy_grid_pipeline", trigger a run
```

Or run the whole DAG once from the command line, with no scheduler needed:

```bash
docker compose run --rm airflow airflow dags test energy_grid_pipeline 2026-11-17
```

Outputs land in `./data/`:

| Path | Contents |
|---|---|
| `data/raw/` | source CSV (downloaded once) |
| `data/curated/hourly/` | clean long-format Parquet, partitioned by `country/year` |
| `data/marts/daily/` | one row per country per day (MWh, renewable share, hours reported) |
| `data/quality/quality_<date>.json` | every check result and the gate decision |
| `data/reports/energy_report_<date>.md` + CSVs | SQL analytics, ready for Tableau or Power BI |

Query the warehouse directly: `psql postgresql://airflow:airflow@localhost:5432/energy -c "SELECT * FROM energy_daily LIMIT 5;"`

## How it works

**Transform (PySpark).** The source has hundreds of wide columns. `to_long` unpivots only the needed series with a single `stack()` expression into `(ts_utc, country, metric, value_mw)`. Values are read as strings and cast explicitly, so malformed readings become countable nulls instead of failing the job. `clean_hourly` drops empty readings and duplicate keys. Negative or implausible values are *kept* so the quality gate can see them.

**Quality gate.** Six checks run on the curated table before anything reaches the mart or the database:

| Check | Rule |
|---|---|
| `row_count` | curated table is not empty |
| `duplicate_keys` | no two rows share `(ts_utc, country, metric)` |
| `negative_generation_rate` | at most 0.1% of solar/wind readings below 0 MW |
| `implausible_values` | no reading above 150,000 MW |
| `country_coverage` | every configured country is present |
| `min_series_completeness` | every series has at least 95% of the hourly readings its time span implies |

`decide_action` turns the results into a decision:
- **fail** (stop the DAG, nothing loaded) on *structural* problems: empty table, duplicate keys, a missing country. Loading those would corrupt the warehouse.
- **warn** (continue, and show it in the report's Data quality section) on *value* problems: spikes, negatives, incomplete series. One bad hour shouldn't stop every downstream dashboard.
- **pass** when every check passes.

On the real data, the gate flags one reading: France's load at 2020-07-07 17:00 UTC is 158,000 MW, while the neighboring hours are about 47,000 MW. The run continues with a warning, and the report lists it.

**Daily mart.** Hourly average MW summed over a day gives MWh. The mart also keeps `hours_reported`, so a short day (missing hours) is visible to anyone reading the numbers.

**Load.** The mart is written to a staging table and upserted into `energy_daily` with `ON CONFLICT (country, date) DO UPDATE`, all inside one transaction.

**Report.** [`sql/`](sql/) holds the analytics, each built on window functions:
- `monthly_renewable_share.sql`: monthly renewable share, month-over-month change (`LAG`), and rank across countries (`RANK`)
- `top_solar_days.sql`: top 3 solar days per country (`ROW_NUMBER`)
- `rolling_load_7d.sql`: 7-day rolling average load (`AVG ... ROWS BETWEEN 6 PRECEDING AND CURRENT ROW`)

## Idempotency

Every task can be retried or re-run without corrupting output:
- **Download:** skipped if a complete file exists. New downloads go to a `.part` file and are renamed only when complete.
- **Parquet:** written with `mode("overwrite")`.
- **Postgres:** upsert keyed on `(country, date)`, in a single transaction. Re-loading the same mart changes nothing.
- **Retries:** two per task, but a schema change upstream raises `AirflowFailException` (no retries), since retrying can't fix it.

## Tests

```bash
pip install -r requirements-dev.txt   # needs Java 17 for Spark
python -m pytest -q
```

The unit tests cover the Spark transforms (unpivot, casting, deduplication, MWh math) and every quality check, using a local Spark session. GitHub Actions runs them on every push.

## Project structure

```
dags/energy_grid_pipeline.py   Airflow DAG (TaskFlow API)
pipeline/                      extract, transform (PySpark), quality, load, report, config
sql/                           analytics queries used by the report
tests/                         pytest + local Spark
docker-compose.yml, Dockerfile Airflow 2.10 + Java + PySpark, Postgres 16
```

## Scaling notes

Spark runs in local mode inside the Airflow container to keep setup to one command. To run on a cluster, set `SPARK_MASTER` (or move the Spark tasks to `SparkSubmitOperator`). The transform code doesn't change.
