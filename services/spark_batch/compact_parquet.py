"""Compact raw Parquet partitions into fewer files."""

from __future__ import annotations

import os
import time
from datetime import date

from pyspark.sql import SparkSession

from services.spark_batch.common import build_spark, partition_path, resolve_trade_date
from services.spark_batch.pg_writer import insert_pipeline_metrics


def compact_day(spark: SparkSession, *, trade_date: date, coalesce_partitions: int) -> int:
    raw_dir = os.getenv("RAW_DATA_DIR", "/data/raw/trades")
    compact_dir = os.getenv("RAW_DATA_COMPACTED_DIR", "/data/raw/trades_compacted")
    source_path = partition_path(raw_dir, trade_date)
    target_path = partition_path(compact_dir, trade_date)

    t0 = time.monotonic()
    try:
        df = spark.read.parquet(source_path)
    except Exception:
        insert_pipeline_metrics(
            job_name="batch_compact_parquet",
            kafka_lag=None,
            batch_duration_ms=int((time.monotonic() - t0) * 1000),
            records_in=0,
            records_out=0,
            records_valid=0,
            records_dlq=0,
            status="skipped_no_data",
        )
        return 0

    records_in = df.count()
    if records_in == 0:
        return 0

    (
        df.dropDuplicates(["event_id"])
        .coalesce(coalesce_partitions)
        .write.mode("overwrite")
        .partitionBy("hour")
        .parquet(target_path)
    )

    insert_pipeline_metrics(
        job_name="batch_compact_parquet",
        kafka_lag=None,
        batch_duration_ms=int((time.monotonic() - t0) * 1000),
        records_in=records_in,
        records_out=records_in,
        records_valid=records_in,
        records_dlq=0,
        status="ok",
    )
    return records_in


def main(trade_date: str | None = None) -> None:
    spark = build_spark("crypto-batch-compact")
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    coalesce_partitions = int(os.getenv("BATCH_COALESCE_PARTITIONS", "1"))
    day = resolve_trade_date(trade_date)
    compact_day(spark, trade_date=day, coalesce_partitions=coalesce_partitions)
    spark.stop()


if __name__ == "__main__":
    import sys

    main(sys.argv[1] if len(sys.argv) > 1 else None)
