"""Pure helpers for REST backfill normalization and validation."""

from __future__ import annotations

from shared.validation import validate_trade


def normalize_agg_trade(item: dict, *, symbol: str, ingested_at_ms: int) -> dict:
    """Normalize Binance aggTrades; event_id matches WebSocket producer format."""
    trade_id = int(item["a"])
    trade_time_ms = int(item["T"])
    price = str(item["p"])
    quantity = str(item["q"])
    symbol_upper = symbol.upper()
    return {
        "event_id": f"{symbol_upper}-{trade_id}-{trade_time_ms}",
        "symbol": symbol_upper,
        "trade_id": trade_id,
        "price": price,
        "quantity": quantity,
        "quote_qty": str(float(price) * float(quantity)),
        "trade_time_ms": trade_time_ms,
        "is_buyer_maker": bool(item.get("m", False)),
        "ingested_at_ms": ingested_at_ms,
        "source": "binance_rest",
    }


def validate_backfill_rows(
    rows: list[dict],
    *,
    allowed_symbols: set[str],
    now_ms: int,
    max_time_skew_ms: int,
) -> tuple[list[dict], int]:
    valid_rows: list[dict] = []
    rejected = 0
    for row in rows:
        result = validate_trade(
            row,
            now_ms=now_ms,
            allowed_symbols=allowed_symbols,
            max_time_skew_ms=max_time_skew_ms,
        )
        if result.is_valid and result.event is not None:
            valid_rows.append(result.event.model_dump())
        else:
            rejected += 1
    return valid_rows, rejected
