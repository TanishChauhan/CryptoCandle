from __future__ import annotations

from shared.aggregation_ref import dedup_by_event_id


def _base_trade(**overrides) -> dict:
    trade = {
        "event_id": "btcusdt-1-1710000000000",
        "symbol": "BTCUSDT",
        "trade_id": 1,
        "price": "67000.00",
        "quantity": "0.100",
        "quote_qty": "6700.00",
        "trade_time_ms": 1710000000000,
    }
    trade.update(overrides)
    return trade


def test_duplicate_event_id_is_dropped() -> None:
    duplicate = _base_trade()
    deduped = dedup_by_event_id([duplicate, duplicate.copy()])
    assert len(deduped) == 1


def test_distinct_event_ids_are_kept() -> None:
    trades = [
        _base_trade(event_id="btcusdt-1-1710000000000", trade_id=1),
        _base_trade(event_id="btcusdt-2-1710000000100", trade_id=2, trade_time_ms=1710000000100),
    ]
    deduped = dedup_by_event_id(trades)
    assert len(deduped) == 2
