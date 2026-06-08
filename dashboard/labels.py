"""User-facing labels for pipeline job names."""

from __future__ import annotations

JOB_DISPLAY_NAMES: dict[str, str] = {
    "spark_raw_valid_trades": "Spark → Parquet",
    "spark_valid_stream": "Spark → Postgres (candles)",
    "spark_invalid_stream": "Spark → DLQ",
    "batch_daily_summary": "Daily summary (batch)",
    "batch_compact": "Parquet compaction (batch)",
}


def friendly_job_name(job_name: str) -> str:
    return JOB_DISPLAY_NAMES.get(str(job_name), str(job_name).replace("_", " ").title())
