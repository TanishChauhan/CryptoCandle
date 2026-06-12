"""PostgreSQL helpers for the Streamlit dashboard."""

from __future__ import annotations

import os
from functools import lru_cache

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

from shared.dq_quality import compute_dlq_ratio, evaluate_dlq_ratio


def _connection_kwargs() -> dict:
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "user": os.getenv("POSTGRES_USER", "crypto"),
        "password": os.getenv("POSTGRES_PASSWORD", "crypto_secret"),
        "dbname": os.getenv("POSTGRES_DB", "crypto_analytics"),
    }


@lru_cache(maxsize=1)
def get_symbols() -> tuple[str, ...]:
    raw = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT")
    return tuple(symbol.strip().upper() for symbol in raw.split(",") if symbol.strip())


def fetch_candles(symbol: str, *, limit: int = 120) -> pd.DataFrame:
    lookback_hours = max(1, int(os.getenv("CANDLES_LOOKBACK_HOURS", "24")))
    sql = """
    SELECT
      window_start,
      window_end,
      open_price,
      high_price,
      low_price,
      close_price,
      avg_price,
      volume,
      quote_volume,
      trade_count,
      volatility
    FROM analytics.candles_1m
    WHERE symbol = %s
      AND window_start >= NOW() - (%s * INTERVAL '1 hour')
    ORDER BY window_start DESC
    LIMIT %s;
    """
    with psycopg2.connect(**_connection_kwargs()) as conn:
        df = pd.read_sql(sql, conn, params=(symbol, lookback_hours, limit))
    if df.empty:
        return df
    return df.sort_values("window_start")


def fetch_pipeline_metrics(*, limit: int = 500, hours: int | None = None) -> pd.DataFrame:
    lookback_hours = max(1, hours if hours is not None else int(os.getenv("PIPELINE_METRICS_LOOKBACK_HOURS", "24")))
    sql = """
    SELECT
      recorded_at,
      job_name,
      kafka_lag,
      batch_duration_ms,
      records_in,
      records_out,
      records_valid,
      records_dlq,
      status
    FROM analytics.pipeline_metrics
    WHERE recorded_at >= NOW() - (%s * INTERVAL '1 hour')
    ORDER BY recorded_at DESC
    LIMIT %s;
    """
    with psycopg2.connect(**_connection_kwargs()) as conn:
        return pd.read_sql(sql, conn, params=(lookback_hours, limit))


def fetch_dlq_summary(hours: int = 24) -> dict:
    sql = """
    SELECT
      COALESCE(SUM(records_in), 0) AS records_in,
      COALESCE(SUM(records_dlq), 0) AS records_dlq
    FROM analytics.pipeline_metrics
    WHERE recorded_at >= NOW() - (%s * INTERVAL '1 hour');
    """
    with psycopg2.connect(**_connection_kwargs()) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (hours,))
            row = cur.fetchone() or {"records_in": 0, "records_dlq": 0}

    records_in = int(row["records_in"])
    records_dlq = int(row["records_dlq"])
    ratio = compute_dlq_ratio(records_in=records_in, records_dlq=records_dlq)
    status, message = evaluate_dlq_ratio(ratio)
    return {
        "records_in": records_in,
        "records_dlq": records_dlq,
        "dlq_ratio": ratio,
        "status": status,
        "message": message,
    }


def fetch_daily_summary(symbol: str, *, limit: int = 14) -> pd.DataFrame:
    sql = """
    SELECT trade_date, vwap, total_volume, high_price, low_price
    FROM analytics.daily_summary
    WHERE symbol = %s
    ORDER BY trade_date DESC
    LIMIT %s;
    """
    with psycopg2.connect(**_connection_kwargs()) as conn:
        df = pd.read_sql(sql, conn, params=(symbol, limit))
    if df.empty:
        return df
    return df.sort_values("trade_date")
