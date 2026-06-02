"""Hourly REST backfill DAG."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def _run_hourly_backfill() -> None:
    from services.spark_batch.backfill_rest import run_hourly_backfill

    valid_count, rejected_count = run_hourly_backfill()
    print(f"hourly_backfill complete: valid={valid_count}, rejected={rejected_count}")


with DAG(
    dag_id="crypto_hourly_backfill",
    description="Hourly Binance REST gap-fill with shared validation",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "crypto-etl",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["crypto", "batch", "backfill"],
) as dag:
    PythonOperator(
        task_id="run_hourly_rest_backfill",
        python_callable=_run_hourly_backfill,
    )
