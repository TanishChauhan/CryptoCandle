"""REST backfill helper: fetch Binance aggTrades, validate, write Parquet."""

from __future__ import annotations

import os
import time

from pyspark.sql import functions as F

from services.spark_batch.backfill_utils import (
    fetch_agg_trades,
    normalize_agg_trade,
    validate_backfill_rows,
)
from services.spark_batch.common import build_spark, load_symbols
from services.spark_batch.pg_writer import insert_pipeline_metrics


def run_hourly_backfill(*, lookback_hours: int = 2) -> tuple[int, int]:
    t0 = time.monotonic()
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - lookback_hours * 60 * 60 * 1000
    now_ms = end_ms
    max_time_skew_ms = int(os.getenv("BACKFILL_MAX_TIME_SKEW_MS", str(7 * 24 * 60 * 60 * 1000)))

    rest_base = os.getenv("BINANCE_REST_BASE", "https://api.binance.com")
    symbols = load_symbols()
    allowed = set(symbols)

    all_valid: list[dict] = []
    rejected_total = 0
    ingested_at_ms = now_ms

    for symbol in symbols:
        trades = fetch_agg_trades(
            symbol=symbol,
            start_ms=start_ms,
            end_ms=end_ms,
            rest_base=rest_base,
        )
        normalized = [
            normalize_agg_trade(item, symbol=symbol, ingested_at_ms=ingested_at_ms) for item in trades
        ]
        valid_rows, rejected = validate_backfill_rows(
            normalized,
            allowed_symbols=allowed,
            now_ms=now_ms,
            max_time_skew_ms=max_time_skew_ms,
        )
        all_valid.extend(valid_rows)
        rejected_total += rejected

    if all_valid:
        spark = build_spark("crypto-batch-backfill")
        spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
        df = spark.createDataFrame(all_valid)
        (
            df.withColumn("year", F.year(F.from_unixtime(F.col("trade_time_ms") / F.lit(1000.0))))
            .withColumn("month", F.month(F.from_unixtime(F.col("trade_time_ms") / F.lit(1000.0))))
            .withColumn("day", F.dayofmonth(F.from_unixtime(F.col("trade_time_ms") / F.lit(1000.0))))
            .withColumn("hour", F.hour(F.from_unixtime(F.col("trade_time_ms") / F.lit(1000.0))))
            .write.mode("append")
            .partitionBy("year", "month", "day", "hour")
            .parquet(os.getenv("RAW_DATA_DIR", "/data/raw/trades"))
        )
        spark.stop()

    insert_pipeline_metrics(
        job_name="batch_hourly_backfill",
        kafka_lag=None,
        batch_duration_ms=int((time.monotonic() - t0) * 1000),
        records_in=len(all_valid) + rejected_total,
        records_out=len(all_valid),
        records_valid=len(all_valid),
        records_dlq=rejected_total,
        status="ok",
    )
    return len(all_valid), rejected_total


def main() -> None:
    lookback_hours = int(os.getenv("BACKFILL_LOOKBACK_HOURS", "2"))
    run_hourly_backfill(lookback_hours=lookback_hours)


if __name__ == "__main__":
    main()
