from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from shared.aggregation_ref import compute_1m_candles


def _load_fixture(name: str) -> list[dict]:
    path = Path(__file__).parent / "fixtures" / name
    return json.loads(path.read_text(encoding="utf-8"))


def test_one_minute_ohlc_volume_and_avg_price() -> None:
    candles = compute_1m_candles(_load_fixture("trades_1m_bucket.json"))
    candle = candles[0]

    assert candle["symbol"] == "BTCUSDT"
    assert candle["open_price"] == Decimal("67000.00")
    assert candle["high_price"] == Decimal("67100.00")
    assert candle["low_price"] == Decimal("66900.00")
    assert candle["close_price"] == Decimal("66900.00")
    assert candle["volume"] == Decimal("0.35")
    assert candle["quote_volume"] == Decimal("23465.00")
    assert candle["trade_count"] == 3
    assert float(candle["avg_price"]) == pytest.approx(67000.0)


def test_one_minute_volatility_is_computed_for_multiple_prices() -> None:
    candle = compute_1m_candles(_load_fixture("trades_1m_bucket.json"))[0]
    assert candle["volatility"] is not None
    assert candle["volatility"] > 0
