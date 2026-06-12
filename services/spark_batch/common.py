"""Shared helpers for PySpark batch jobs."""

from __future__ import annotations

import os
from datetime import UTC, date, datetime, timedelta

from pyspark.sql import SparkSession


def load_symbols() -> list[str]:
    raw = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT")
    return [symbol.strip().upper() for symbol in raw.split(",") if symbol.strip()]


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def resolve_trade_date(value: str | None = None) -> date:
    if value:
        return date.fromisoformat(value)
    return (datetime.now(UTC) - timedelta(days=1)).date()


def partition_path(base_dir: str, trade_date: date) -> str:
    return (
        f"{base_dir.rstrip('/')}/"
        f"year={trade_date.year}/month={trade_date.month}/day={trade_date.day}"
    )
