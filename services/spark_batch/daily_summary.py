"""Build daily analytics summary from raw/compacted Parquet using PySpark DataFrame API."""

from __future__ import annotations

import os
import time
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from services.spark_batch.common import build_spark, partition_path, resolve_trade_date
from services.spark_batch.pg_writer import insert_pipeline_metrics, upsert_daily_summary


def read_trades_for_day(spark: SparkSession, trade_date: date):
    compact_dir = os.getenv("RAW_DATA_COMPACTED_DIR", "/data/raw/trades_compacted")
    raw_dir = os.getenv("RAW_DATA_DIR", "/data/raw/trades")
    compact_path = partition_path(compact_dir, trade_date)
    raw_path = partition_path(raw_dir, trade_date)

    for path in (compact_path, raw_path):
        try:
            df = spark.read.parquet(path)
            if df.head(1):
                return df
        except Exception:
            continue
    return None


def build_daily_summary_df(df):
    priced = (
        df.withColumn("price_dec", F.col("price").cast(DecimalType(20, 8)))
        .withColumn("quantity_dec", F.col("quantity").cast(DecimalType(38, 12)))
        .withColumn("trade_time", F.to_timestamp(F.col("trade_time_ms") / F.lit(1000.0)))
    )

    return (
        priced.groupBy(F.col("symbol"), F.to_date(F.col("trade_time")).alias("trade_date"))
        .agg(
            (
                F.sum(F.col("price_dec") * F.col("quantity_dec"))
                / F.when(F.sum(F.col("quantity_dec")) == 0, F.lit(None)).otherwise(
                    F.sum(F.col("quantity_dec"))
                )
            ).alias("vwap"),
            F.sum(F.col("quantity_dec")).alias("total_volume"),
            F.max(F.col("price_dec")).alias("high_price"),
            F.min(F.col("price_dec")).alias("low_price"),
        )
    )


def run_daily_summary(spark: SparkSession, *, trade_date: date) -> int:
    t0 = time.monotonic()
    source_df = read_trades_for_day(spark, trade_date)
    if source_df is None:
        insert_pipeline_metrics(
            job_name="batch_daily_summary",
            kafka_lag=None,
            batch_duration_ms=int((time.monotonic() - t0) * 1000),
            records_in=0,
            records_out=0,
            records_valid=0,
            records_dlq=0,
            status="skipped_no_data",
        )
        return 0

    records_in = source_df.count()
    summary_df = build_daily_summary_df(source_df)
    rows = [
        (
            row["trade_date"],
            row["symbol"],
            row["vwap"],
            row["total_volume"],
            row["high_price"],
            row["low_price"],
        )
        for row in summary_df.collect()
    ]
    upsert_daily_summary(rows)

    insert_pipeline_metrics(
        job_name="batch_daily_summary",
        kafka_lag=None,
        batch_duration_ms=int((time.monotonic() - t0) * 1000),
        records_in=records_in,
        records_out=len(rows),
        records_valid=len(rows),
        records_dlq=0,
        status="ok",
    )
    return len(rows)


def main(trade_date: str | None = None) -> None:
    spark = build_spark("crypto-batch-daily-summary")
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    day = resolve_trade_date(trade_date)
    run_daily_summary(spark, trade_date=day)
    spark.stop()


if __name__ == "__main__":
    import sys

    main(sys.argv[1] if len(sys.argv) > 1 else None)
