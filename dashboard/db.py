"""PostgreSQL helpers for the Streamlit dashboard."""

from __future__ import annotations

import json
import os
import time
from functools import lru_cache
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

from shared.dq_quality import compute_dlq_ratio, evaluate_dlq_ratio

_DEBUG_LOG_PATHS = (
    Path("/app/debug-6be8fb.log"),
    Path(__file__).resolve().parents[1] / "debug-6be8fb.log",
)


def _debug_log(hypothesis_id: str, location: str, message: str, data: dict) -> None:
    # #region agent log
    payload = {
        "sessionId": "6be8fb",
        "runId": os.getenv("DEBUG_RUN_ID", "pre-fix"),
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "timestamp": int(time.time() * 1000),
    }
    line = json.dumps(payload, default=str) + "\n"
    for path in _DEBUG_LOG_PATHS:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(line)
            break
        except OSError:
            continue
    # #endregion


def _probe_schema(conn_kwargs: dict) -> dict:
    sql = """
    SELECT
      current_database() AS db_name,
      EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'analytics') AS analytics_schema_exists,
      EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'analytics' AND table_name = 'candles_1m'
      ) AS candles_table_exists,
      (
        SELECT COUNT(*)::int
        FROM information_schema.tables
        WHERE table_schema = 'analytics'
      ) AS analytics_table_count
    """
    with psycopg2.connect(**conn_kwargs) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            return dict(cur.fetchone() or {})


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
    raw = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT")
    return tuple(symbol.strip().upper() for symbol in raw.split(",") if symbol.strip())


def fetch_candles(symbol: str, *, limit: int = 120) -> pd.DataFrame:
    conn_kwargs = _connection_kwargs()
    safe_conn = {k: v for k, v in conn_kwargs.items() if k != "password"}
    try:
        probe = _probe_schema(conn_kwargs)
        _debug_log(
            "H1",
            "dashboard/db.py:fetch_candles",
            "schema_probe_before_query",
            {"conn": safe_conn, "symbol": symbol, **probe},
        )
    except Exception as exc:
        _debug_log(
            "H2",
            "dashboard/db.py:fetch_candles",
            "schema_probe_failed",
            {"conn": safe_conn, "symbol": symbol, "error": repr(exc)},
        )

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
    ORDER BY window_start DESC
    LIMIT %s;
    """
    try:
        with psycopg2.connect(**conn_kwargs) as conn:
            df = pd.read_sql(sql, conn, params=(symbol, limit))
    except Exception as exc:
        _debug_log(
            "H3",
            "dashboard/db.py:fetch_candles",
            "fetch_candles_query_failed",
            {"conn": safe_conn, "symbol": symbol, "error": repr(exc)},
        )
        raise
    if df.empty:
        return df
    return df.sort_values("window_start")


def fetch_pipeline_metrics(*, limit: int = 100) -> pd.DataFrame:
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
    ORDER BY recorded_at DESC
    LIMIT %s;
    """
    with psycopg2.connect(**_connection_kwargs()) as conn:
        return pd.read_sql(sql, conn, params=(limit,))


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
