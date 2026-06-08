"""Pure helpers for REST backfill normalization, pagination, and validation."""

from __future__ import annotations

import httpx

from shared.validation import validate_trade


def fetch_agg_trades_page(
    *,
    symbol: str,
    start_ms: int,
    end_ms: int,
    rest_base: str,
    limit: int = 1000,
    client: httpx.Client | None = None,
) -> list[dict]:
    """Fetch one page (up to ``limit`` rows) from Binance aggTrades."""
    params = {"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": limit}
    if client is not None:
        response = client.get("/api/v3/aggTrades", params=params)
    else:
        with httpx.Client(base_url=rest_base, timeout=30.0) as owned:
            response = owned.get("/api/v3/aggTrades", params=params)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        return []
    return payload


def fetch_agg_trades(
    *,
    symbol: str,
    start_ms: int,
    end_ms: int,
    rest_base: str,
    limit: int = 1000,
) -> list[dict]:
    """Fetch all aggTrades in [start_ms, end_ms] with pagination."""
    all_trades: list[dict] = []
    cursor = start_ms

    with httpx.Client(base_url=rest_base, timeout=30.0) as client:
        while cursor < end_ms:
            page = fetch_agg_trades_page(
                symbol=symbol,
                start_ms=cursor,
                end_ms=end_ms,
                rest_base=rest_base,
                limit=limit,
                client=client,
            )
            if not page:
                break

            all_trades.extend(page)
            last_ts = int(page[-1]["T"])
            if len(page) < limit:
                break

            next_cursor = last_ts + 1
            if next_cursor <= cursor:
                break
            cursor = next_cursor

    return all_trades


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
