"""Automated reporting: run the SQL analytics in sql/ and write a Markdown report + CSVs for BI tools."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

REPORT_QUERIES = {
    "Monthly renewable share (latest months)": "monthly_renewable_share.sql",
    "Top solar days per country": "top_solar_days.sql",
    "7-day rolling load (latest 14 days)": "rolling_load_7d.sql",
}


def _markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_no rows_\n"
    header = "| " + " | ".join(df.columns) + " |"
    sep = "| " + " | ".join("---" for _ in df.columns) + " |"
    rows = ["| " + " | ".join("" if pd.isna(v) else str(v) for v in row) + " |" for row in df.itertuples(index=False)]
    return "\n".join([header, sep, *rows]) + "\n"


def _quality_section(quality_path: Path | None) -> str:
    if not quality_path or not quality_path.exists():
        return "## Data quality\n\n_no quality results found for this run_\n"
    payload = json.loads(quality_path.read_text())
    checks = pd.DataFrame(payload["checks"])
    checks["status"] = checks["passed"].map({True: "PASS", False: "FAIL"})
    table = _markdown_table(checks[["name", "status", "observed", "threshold", "detail"]])
    return f"## Data quality\n\nGate decision: **{payload['action'].upper()}**\n\n{table}"


def build_report(db_uri: str, sql_dir: Path, out_dir: Path, run_date: str | None = None,
                 quality_path: Path | None = None) -> Path:
    run_date = run_date or date.today().isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    engine = create_engine(db_uri)
    sections = [f"# Energy grid report - {run_date}\n",
                "Source: Open Power System Data (ENTSO-E transparency data), hourly, 2015-2020.\n",
                _quality_section(quality_path)]
    with engine.connect() as conn:
        for title, filename in REPORT_QUERIES.items():
            df = pd.read_sql((sql_dir / filename).read_text(), conn)
            df.to_csv(out_dir / f"{Path(filename).stem}_{run_date}.csv", index=False)
            sections.append(f"## {title}\n\n{_markdown_table(df)}")
    path = out_dir / f"energy_report_{run_date}.md"
    path.write_text("\n".join(sections))
    return path
