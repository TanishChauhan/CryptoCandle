"""Daily compaction + daily summary DAG."""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def _run_daily_batch() -> None:
    from services.spark_batch.common import build_spark, resolve_trade_date
    from services.spark_batch.compact_parquet import compact_day
    from services.spark_batch.daily_summary import run_daily_summary

    trade_date = resolve_trade_date(None)
    coalesce_partitions = int(os.getenv("BATCH_COALESCE_PARTITIONS", "1"))

    spark = build_spark("crypto-airflow-daily-batch")
    try:
        compacted = compact_day(
            spark,
            trade_date=trade_date,
            coalesce_partitions=coalesce_partitions,
        )
        summaries = run_daily_summary(spark, trade_date=trade_date)
        print(
            f"daily_batch complete for {trade_date}: "
            f"compacted_rows={compacted}, summary_rows={summaries}"
        )
    finally:
        spark.stop()


with DAG(
    dag_id="crypto_daily_batch",
    description="Daily Parquet compaction and analytics.daily_summary rebuild",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "crypto-etl",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["crypto", "batch", "daily"],
) as dag:
    PythonOperator(
        task_id="run_daily_compact_and_summary",
        python_callable=_run_daily_batch,
    )
