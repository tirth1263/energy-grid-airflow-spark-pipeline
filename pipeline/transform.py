"""PySpark transforms: wide hourly CSV -> clean long-format table -> daily energy mart."""
from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.config import COUNTRY_COLUMNS, METRICS


def get_spark(app_name: str = "energy-grid-pipeline") -> SparkSession:
    """Local-mode Spark session. Point SPARK_MASTER at a cluster to run the same code distributed."""
    return (
        SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[*]"))
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        .getOrCreate()
    )


def read_raw(spark: SparkSession, path: str) -> DataFrame:
    # Read everything as strings; types are cast explicitly in to_long so bad values become nulls
    # we can count, instead of failing the whole read.
    return spark.read.option("header", True).csv(path)


def to_long(raw: DataFrame, country_columns: dict[str, dict[str, str]] = COUNTRY_COLUMNS) -> DataFrame:
    """Unpivot the selected wide columns into (ts_utc, country, metric, value_mw)."""
    pairs = [(c, m, col) for c, metrics in country_columns.items() for m, col in metrics.items()]
    stack_args = ", ".join(f"'{c}', '{m}', `{col}`" for c, m, col in pairs)
    stacked = raw.select(
        F.col("utc_timestamp"),
        F.expr(f"stack({len(pairs)}, {stack_args}) as (country, metric, value_raw)"),
    )
    return (
        stacked.withColumn("ts_utc", F.to_timestamp("utc_timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("value_mw", F.col("value_raw").cast("double"))
        .select("ts_utc", "country", "metric", "value_mw")
    )


def clean_hourly(long_df: DataFrame) -> DataFrame:
    """Drop rows with no timestamp or value, remove duplicate keys, add partition columns.

    Negative or implausible values are deliberately *kept* here: the quality gate counts them
    and decides whether the run may continue, rather than this step hiding them.
    """
    return (
        long_df.where(F.col("ts_utc").isNotNull() & F.col("value_mw").isNotNull())
        .dropDuplicates(["ts_utc", "country", "metric"])
        .withColumn("date", F.to_date("ts_utc"))
        .withColumn("year", F.year("ts_utc"))
    )


def daily_energy(hourly: DataFrame) -> DataFrame:
    """One row per country per day. Hourly-average MW summed over a day gives MWh."""
    sums = (
        hourly.groupBy("country", "date")
        .pivot("metric", list(METRICS))
        .agg(F.sum("value_mw"))
    )
    hours = hourly.groupBy("country", "date").agg(F.countDistinct("ts_utc").alias("hours_reported"))
    daily = sums.join(hours, ["country", "date"])
    for m in METRICS:
        daily = daily.withColumnRenamed(m, f"{m}_mwh")
    renewable = F.coalesce(F.col("solar_mwh"), F.lit(0.0)) + F.coalesce(F.col("wind_mwh"), F.lit(0.0))
    return daily.withColumn(
        "renewable_share",
        F.when(F.col("load_mwh") > 0, F.round(renewable / F.col("load_mwh"), 4)),
    ).select("country", "date", "load_mwh", "solar_mwh", "wind_mwh", "renewable_share", "hours_reported")


def write_hourly(hourly: DataFrame, path: str) -> None:
    hourly.write.mode("overwrite").partitionBy("country", "year").parquet(path)


def write_daily(daily: DataFrame, path: str) -> None:
    # The mart is small (a few thousand rows), so one file keeps the downstream load simple.
    daily.coalesce(1).write.mode("overwrite").parquet(path)
