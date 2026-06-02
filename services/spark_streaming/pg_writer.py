"""PostgreSQL writers for streaming micro-batches."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg2
from psycopg2.extras import execute_values


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    user: str
    password: str
    dbname: str


def load_postgres_config() -> PostgresConfig:
    return PostgresConfig(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "crypto"),
        password=os.getenv("POSTGRES_PASSWORD", "crypto_secret"),
        dbname=os.getenv("POSTGRES_DB", "crypto_analytics"),
    )


def connect_postgres(cfg: PostgresConfig | None = None) -> psycopg2.extensions.connection:
    """Open a PostgreSQL connection using env-driven defaults."""
    resolved = cfg or load_postgres_config()
    return psycopg2.connect(
        host=resolved.host,
        port=resolved.port,
        user=resolved.user,
        password=resolved.password,
        dbname=resolved.dbname,
    )


def _connect(cfg: PostgresConfig) -> psycopg2.extensions.connection:
    return connect_postgres(cfg)


def upsert_candles_1m(rows: list[tuple[Any, ...]]) -> None:
    """
    Upsert 1-minute candles.

    rows must match the column order defined in the INSERT below:
      (symbol, window_start, window_end, open_price, avg_price, high_price, low_price,
       close_price, volume, quote_volume, trade_count, volatility)
    """
    if not rows:
        return

    sql = """
    INSERT INTO analytics.candles_1m (
      symbol, window_start, window_end,
      open_price, avg_price, high_price, low_price, close_price,
      volume, quote_volume,
      trade_count, volatility
    )
    VALUES %s
    ON CONFLICT (symbol, window_start) DO UPDATE SET
      window_end = EXCLUDED.window_end,
      open_price = EXCLUDED.open_price,
      avg_price = EXCLUDED.avg_price,
      high_price = EXCLUDED.high_price,
      low_price = EXCLUDED.low_price,
      close_price = EXCLUDED.close_price,
      volume = EXCLUDED.volume,
      quote_volume = EXCLUDED.quote_volume,
      trade_count = EXCLUDED.trade_count,
      volatility = EXCLUDED.volatility,
      updated_at = NOW();
    """

    cfg = load_postgres_config()
    with _connect(cfg) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
        conn.commit()


def insert_pipeline_metrics(
    *,
    job_name: str,
    kafka_lag: int | None,
    batch_duration_ms: int,
    records_in: int,
    records_out: int,
    records_valid: int,
    records_dlq: int,
    status: str = "ok",
) -> None:
    warn_ratio = float(os.getenv("DLQ_WARN_RATIO", "0.01"))
    fail_ratio = float(os.getenv("DLQ_FAIL_RATIO", "0.05"))

    sql = """
    INSERT INTO analytics.pipeline_metrics (
      recorded_at,
      job_name,
      kafka_lag,
      batch_duration_ms,
      records_in,
      records_out,
      records_valid,
      records_dlq,
      dlq_warn_ratio,
      dlq_fail_ratio,
      status
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """

    cfg = load_postgres_config()
    with _connect(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    datetime.now(timezone.utc),
                    job_name,
                    kafka_lag,
                    batch_duration_ms,
                    records_in,
                    records_out,
                    records_valid,
                    records_dlq,
                    warn_ratio,
                    fail_ratio,
                    status,
                ),
            )
        conn.commit()

