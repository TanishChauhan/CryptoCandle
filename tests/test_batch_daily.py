from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from shared.aggregation_ref import compute_daily_summary


def test_daily_summary_vwap_high_low() -> None:
    day = datetime(2024, 3, 10, tzinfo=UTC).date()
    base_ms = int(datetime(2024, 3, 10, 12, 0, tzinfo=UTC).timestamp() * 1000)
    trades = [
        {
            "symbol": "BTCUSDT",
            "price": "100.00",
            "quantity": "1.0",
            "trade_time_ms": base_ms,
        },
        {
            "symbol": "BTCUSDT",
            "price": "200.00",
            "quantity": "3.0",
            "trade_time_ms": base_ms + 1000,
        },
    ]

    summary = compute_daily_summary(trades)[0]
    assert summary["trade_date"] == day
    assert summary["symbol"] == "BTCUSDT"
    assert summary["high_price"] == Decimal("200.00")
    assert summary["low_price"] == Decimal("100.00")
    assert summary["total_volume"] == Decimal("4.0")
    assert float(summary["vwap"]) == 175.0
