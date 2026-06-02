"""Pure-Python reference implementations for aggregation and dedup tests."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, date, datetime
from decimal import Decimal
from statistics import mean, stdev


def floor_to_minute_ms(trade_time_ms: int) -> int:
    dt = datetime.fromtimestamp(trade_time_ms / 1000, tz=UTC).replace(second=0, microsecond=0)
    return int(dt.timestamp() * 1000)


def dedup_by_event_id(trades: list[dict]) -> list[dict]:
    seen: set[str] = set()
    deduped: list[dict] = []
    for trade in trades:
        event_id = trade["event_id"]
        if event_id in seen:
            continue
        seen.add(event_id)
        deduped.append(trade)
    return deduped


def compute_1m_candles(trades: list[dict]) -> list[dict]:
    """Mirror Spark 1-minute OHLC semantics for unit tests."""
    buckets: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for trade in trades:
        bucket_start = floor_to_minute_ms(int(trade["trade_time_ms"]))
        buckets[(trade["symbol"], bucket_start)].append(trade)

    candles: list[dict] = []
    for (symbol, bucket_start), bucket_trades in buckets.items():
        ordered = sorted(bucket_trades, key=lambda row: int(row["trade_time_ms"]))
        prices = [Decimal(str(row["price"])) for row in ordered]
        quantities = [Decimal(str(row["quantity"])) for row in ordered]
        quote_qtys = [Decimal(str(row.get("quote_qty") or "0")) for row in ordered]
        price_floats = [float(price) for price in prices]

        candles.append(
            {
                "symbol": symbol,
                "window_start_ms": bucket_start,
                "window_end_ms": bucket_start + 60_000,
                "open_price": prices[0],
                "close_price": prices[-1],
                "high_price": max(prices),
                "low_price": min(prices),
                "avg_price": Decimal(str(mean(price_floats))),
                "volume": sum(quantities),
                "quote_volume": sum(quote_qtys),
                "trade_count": len(ordered),
                "volatility": stdev(price_floats) if len(price_floats) > 1 else None,
            }
        )

    return sorted(candles, key=lambda row: (row["symbol"], row["window_start_ms"]))


def compute_daily_summary(trades: list[dict]) -> list[dict]:
    """Mirror Spark daily summary semantics for unit tests."""
    buckets: dict[tuple[str, date], list[dict]] = defaultdict(list)
    for trade in trades:
        trade_day = datetime.fromtimestamp(int(trade["trade_time_ms"]) / 1000, tz=UTC).date()
        buckets[(trade["symbol"], trade_day)].append(trade)

    summaries: list[dict] = []
    for (symbol, trade_date), bucket_trades in buckets.items():
        prices = [Decimal(str(row["price"])) for row in bucket_trades]
        quantities = [Decimal(str(row["quantity"])) for row in bucket_trades]
        total_volume = sum(quantities)
        quote_notional = sum(price * qty for price, qty in zip(prices, quantities, strict=True))
        vwap = quote_notional / total_volume if total_volume > 0 else Decimal("0")

        summaries.append(
            {
                "trade_date": trade_date,
                "symbol": symbol,
                "vwap": vwap,
                "total_volume": total_volume,
                "high_price": max(prices),
                "low_price": min(prices),
            }
        )

    return sorted(summaries, key=lambda row: (row["trade_date"], row["symbol"]))
