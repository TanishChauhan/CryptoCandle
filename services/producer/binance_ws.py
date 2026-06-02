"""Binance WebSocket helpers for trade streams."""

from __future__ import annotations

import json
from typing import Any

import websockets
from websockets.legacy.client import WebSocketClientProtocol


def build_combined_trade_stream_url(base_url: str, symbols: list[str]) -> str:
    streams = "/".join(f"{symbol.lower()}@trade" for symbol in symbols)
    sanitized_base = base_url.rstrip("/")
    return f"{sanitized_base}/stream?streams={streams}"


def normalize_trade_payload(payload: dict[str, Any], *, ingested_at_ms: int) -> dict[str, Any]:
    data = payload.get("data", payload)
    symbol = str(data["s"]).upper()
    trade_id = int(data["t"])
    trade_time_ms = int(data["T"])

    return {
        "event_id": f"{symbol}-{trade_id}-{trade_time_ms}",
        "symbol": symbol,
        "trade_id": trade_id,
        "price": str(data["p"]),
        "quantity": str(data["q"]),
        "quote_qty": None,
        "trade_time_ms": trade_time_ms,
        "is_buyer_maker": bool(data.get("m", False)),
        "ingested_at_ms": ingested_at_ms,
        "source": "binance_ws",
    }


async def recv_json(ws: WebSocketClientProtocol) -> dict[str, Any]:
    raw_message = await ws.recv()
    if not isinstance(raw_message, str):
        raise ValueError("non-text websocket payload")
    return json.loads(raw_message)


async def connect(url: str) -> WebSocketClientProtocol:
    return await websockets.connect(
        url,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=10,
        max_queue=1000,
    )
