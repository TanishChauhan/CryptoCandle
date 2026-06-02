from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.fixture
def sample_trade() -> dict:
    return {
        "event_id": "btcusdt-123-1710000000000",
        "symbol": "BTCUSDT",
        "trade_id": 123,
        "price": "67234.50",
        "quantity": "0.010",
        "quote_qty": "672.345",
        "trade_time_ms": 1710000000000,
        "is_buyer_maker": True,
        "ingested_at_ms": 1710000000500,
        "source": "binance_ws",
    }


@pytest.fixture
def sample_trades_fixture() -> list[dict]:
    fixture_path = Path(__file__).parent / "fixtures" / "trades_sample.json"
    return json.loads(fixture_path.read_text(encoding="utf-8"))

