"""Daily data quality checks DAG."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def _run_data_quality_checks() -> None:
    from services.spark_batch.data_quality import run_dlq_quality_check

    result = run_dlq_quality_check(lookback_hours=24)
    print(
        "dlq_quality_check:",
        f"records_in={result.records_in}",
        f"records_dlq={result.records_dlq}",
        f"dlq_ratio={result.dlq_ratio:.6f}",
        f"status={result.status}",
        f"message={result.message}",
    )
    if result.status == "fail":
        raise RuntimeError(result.message)
    if result.status == "warn":
        print(f"WARNING: {result.message}")


with DAG(
    dag_id="crypto_data_quality",
    description="Daily DLQ ratio and pipeline quality checks",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "crypto-etl",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["crypto", "quality"],
) as dag:
    PythonOperator(
        task_id="check_dlq_ratio",
        python_callable=_run_data_quality_checks,
    )
