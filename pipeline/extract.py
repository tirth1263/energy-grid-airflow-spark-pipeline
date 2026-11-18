"""Extract step: download the raw CSV idempotently and validate its schema."""
from __future__ import annotations

import csv
from pathlib import Path

import requests

from pipeline.config import COUNTRY_COLUMNS, RAW_FILE, RAW_MIN_BYTES, RAW_URL


def download_raw(url: str = RAW_URL, dest: Path = RAW_FILE, min_bytes: int = RAW_MIN_BYTES) -> dict:
    """Download the source file once.

    Idempotent: if a complete file already exists it is reused. The download goes to a
    `.part` file and is renamed only when finished, so a failed or retried task never
    leaves a truncated CSV behind for the next step to read.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size >= min_bytes:
        return {"path": str(dest), "bytes": dest.stat().st_size, "downloaded": False}

    tmp = dest.with_suffix(dest.suffix + ".part")
    with requests.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        with open(tmp, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                fh.write(chunk)

    size = tmp.stat().st_size
    if size < min_bytes:
        tmp.unlink(missing_ok=True)
        raise ValueError(f"Downloaded file is only {size} bytes; expected at least {min_bytes}")
    tmp.replace(dest)
    return {"path": str(dest), "bytes": size, "downloaded": True}


def required_columns(country_columns: dict[str, dict[str, str]] = COUNTRY_COLUMNS) -> list[str]:
    cols = ["utc_timestamp"]
    for metrics in country_columns.values():
        cols.extend(metrics.values())
    return cols


def missing_columns(path: Path, country_columns: dict[str, dict[str, str]] = COUNTRY_COLUMNS) -> list[str]:
    """Return required columns that are absent from the CSV header (empty list = schema OK)."""
    with open(path, newline="", encoding="utf-8") as fh:
        header = next(csv.reader(fh))
    present = set(header)
    return [c for c in required_columns(country_columns) if c not in present]
