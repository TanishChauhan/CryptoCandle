"""PostgreSQL writers for batch job outputs."""

from __future__ import annotations

from typing import Any

from services.spark_streaming.pg_writer import _connect, insert_pipeline_metrics, load_postgres_config
from psycopg2.extras import execute_values


def upsert_daily_summary(rows: list[tuple[Any, ...]]) -> None:
    """
    Upsert daily summary rows.

    Row order:
      (trade_date, symbol, vwap, total_volume, high_price, low_price)
    """
    if not rows:
        return

    sql = """
    INSERT INTO analytics.daily_summary (
      trade_date, symbol, vwap, total_volume, high_price, low_price
    )
    VALUES %s
    ON CONFLICT (trade_date, symbol) DO UPDATE SET
      vwap = EXCLUDED.vwap,
      total_volume = EXCLUDED.total_volume,
      high_price = EXCLUDED.high_price,
      low_price = EXCLUDED.low_price;
    """

    cfg = load_postgres_config()
    with _connect(cfg) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
        conn.commit()


__all__ = ["upsert_daily_summary", "insert_pipeline_metrics"]
