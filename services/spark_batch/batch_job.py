"""Batch orchestration entrypoint for compaction + daily summary jobs."""

from __future__ import annotations

import argparse
import os

from services.spark_batch.backfill_rest import run_hourly_backfill
from services.spark_batch.common import build_spark, resolve_trade_date
from services.spark_batch.compact_parquet import compact_day
from services.spark_batch.daily_summary import run_daily_summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run crypto batch jobs")
    parser.add_argument(
        "job",
        choices=["compact", "daily", "backfill", "all"],
        help="Batch job to execute",
    )
    parser.add_argument(
        "--date",
        dest="trade_date",
        default=None,
        help="Trade date (YYYY-MM-DD). Defaults to yesterday for compact/daily.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    trade_date = resolve_trade_date(args.trade_date)

    if args.job == "backfill":
        run_hourly_backfill()
        return

    spark = build_spark("crypto-batch-job")
    coalesce_partitions = int(os.getenv("BATCH_COALESCE_PARTITIONS", "1"))
    try:
        if args.job in {"compact", "all"}:
            compact_day(spark, trade_date=trade_date, coalesce_partitions=coalesce_partitions)
        if args.job in {"daily", "all"}:
            run_daily_summary(spark, trade_date=trade_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
