"""Daily Postgres retention: summarize old 1m candles, then delete."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def _run_retention() -> None:
    import os

    from shared.retention import purge_old_candles

    keep_days = int(os.getenv("RETENTION_KEEP_DAYS", "1"))
    summarize_first = os.getenv("RETENTION_SUMMARIZE_FIRST", "true").lower() in {"1", "true", "yes", "on"}
    metrics_keep_days = int(os.getenv("RETENTION_METRICS_KEEP_DAYS", "7"))

    result = purge_old_candles(
        keep_days=keep_days,
        summarize_first=summarize_first,
        metrics_keep_days=metrics_keep_days,
    )
    print(
        "retention_complete:",
        f"cutoff={result.cutoff_iso}",
        f"daily_summary_rows={result.daily_summary_rows}",
        f"candles_deleted={result.candles_deleted}",
        f"metrics_deleted={result.metrics_deleted}",
    )


with DAG(
    dag_id="crypto_data_retention",
    description="Roll up and purge old 1m candles to control Postgres storage",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "crypto-etl",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["crypto", "retention"],
) as dag:
    PythonOperator(
        task_id="purge_old_candles",
        python_callable=_run_retention,
    )
