from services.spark_batch.backfill_utils import normalize_agg_trade, validate_backfill_rows


def test_normalize_agg_trade_shape() -> None:
    row = normalize_agg_trade(
        {"a": 99, "p": "42000.50", "q": "0.25", "T": 1710000000000, "m": True},
        symbol="BTCUSDT",
        ingested_at_ms=1710000000500,
    )
    assert row["event_id"] == "BTCUSDT-99-1710000000000"
    assert row["symbol"] == "BTCUSDT"
    assert row["price"] == "42000.50"
    assert row["source"] == "binance_rest"


def test_validate_backfill_rejects_negative_quantity() -> None:
    rows = [
        {
            "event_id": "btcusdt-1-1710000000000",
            "symbol": "BTCUSDT",
            "trade_id": 1,
            "price": "100.00",
            "quantity": "-1.0",
            "quote_qty": "100.00",
            "trade_time_ms": 1710000000000,
            "is_buyer_maker": True,
            "ingested_at_ms": 1710000000000,
            "source": "binance_rest",
        }
    ]
    valid, rejected = validate_backfill_rows(
        rows,
        allowed_symbols={"BTCUSDT"},
        now_ms=1710000001000,
        max_time_skew_ms=24 * 60 * 60 * 1000,
    )
    assert valid == []
    assert rejected == 1
