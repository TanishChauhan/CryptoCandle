"""Postgres retention: roll up old 1m candles to daily_summary, then delete."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import psycopg2


@dataclass(frozen=True)
class RetentionResult:
    cutoff_iso: str
    daily_summary_rows: int
    candles_deleted: int
    metrics_deleted: int


def _connection_kwargs() -> dict[str, Any]:
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "user": os.getenv("POSTGRES_USER", "crypto"),
        "password": os.getenv("POSTGRES_PASSWORD", "crypto_secret"),
        "dbname": os.getenv("POSTGRES_DB", "crypto_analytics"),
    }


_SUMMARIZE_SQL = """
INSERT INTO analytics.daily_summary (trade_date, symbol, vwap, total_volume, high_price, low_price)
SELECT
  (window_start AT TIME ZONE 'UTC')::date AS trade_date,
  symbol,
  CASE WHEN SUM(volume) > 0 THEN SUM(quote_volume) / SUM(volume) ELSE NULL END AS vwap,
  SUM(volume) AS total_volume,
  MAX(high_price) AS high_price,
  MIN(low_price) AS low_price
FROM analytics.candles_1m
WHERE window_start < NOW() - (%s * INTERVAL '1 day')
GROUP BY 1, 2
ON CONFLICT (trade_date, symbol) DO UPDATE SET
  vwap = EXCLUDED.vwap,
  total_volume = EXCLUDED.total_volume,
  high_price = EXCLUDED.high_price,
  low_price = EXCLUDED.low_price;
"""

_DELETE_CANDLES_SQL = """
DELETE FROM analytics.candles_1m
WHERE window_start < NOW() - (%s * INTERVAL '1 day');
"""

_DELETE_METRICS_SQL = """
DELETE FROM analytics.pipeline_metrics
WHERE recorded_at < NOW() - (%s * INTERVAL '1 day');
"""

_CUTOFF_SQL = """
SELECT (NOW() - (%s * INTERVAL '1 day')) AT TIME ZONE 'UTC';
"""


def purge_old_candles(
    *,
    keep_days: int = 1,
    summarize_first: bool = True,
    metrics_keep_days: int = 7,
) -> RetentionResult:
    """
    Delete 1m candles older than ``keep_days``.

    When ``summarize_first`` is True, aggregate those rows into ``analytics.daily_summary`` first.
    """
    keep_days = max(0, keep_days)
    metrics_keep_days = max(1, metrics_keep_days)

    with psycopg2.connect(**_connection_kwargs()) as conn:
        with conn.cursor() as cur:
            cur.execute(_CUTOFF_SQL, (keep_days,))
            cutoff_row = cur.fetchone()
            if cutoff_row is None:
                raise RuntimeError("failed to compute retention cutoff")
            cutoff_iso = cutoff_row[0].isoformat()

            daily_summary_rows = 0
            if summarize_first:
                cur.execute(_SUMMARIZE_SQL, (keep_days,))
                daily_summary_rows = cur.rowcount

            cur.execute(_DELETE_CANDLES_SQL, (keep_days,))
            candles_deleted = cur.rowcount

            cur.execute(_DELETE_METRICS_SQL, (metrics_keep_days,))
            metrics_deleted = cur.rowcount

        conn.commit()

    return RetentionResult(
        cutoff_iso=cutoff_iso,
        daily_summary_rows=daily_summary_rows,
        candles_deleted=candles_deleted,
        metrics_deleted=metrics_deleted,
    )
