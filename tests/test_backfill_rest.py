from unittest.mock import MagicMock, patch

from services.spark_batch.backfill_utils import (
    fetch_agg_trades,
    fetch_agg_trades_page,
    normalize_agg_trade,
    validate_backfill_rows,
)


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


def test_fetch_agg_trades_paginates_until_short_page() -> None:
    page1 = [{"a": i, "p": "1", "q": "1", "T": 1_000 + i, "m": False} for i in range(1000)]
    page2 = [{"a": 2000, "p": "1", "q": "1", "T": 2_500, "m": False}]

    mock_client = MagicMock()
    mock_response_1 = MagicMock()
    mock_response_1.raise_for_status.return_value = None
    mock_response_1.json.return_value = page1
    mock_response_2 = MagicMock()
    mock_response_2.raise_for_status.return_value = None
    mock_response_2.json.return_value = page2
    mock_client.get.side_effect = [mock_response_1, mock_response_2]

    with patch("services.spark_batch.backfill_utils.httpx.Client") as mock_client_cls:
        mock_client_cls.return_value.__enter__.return_value = mock_client
        trades = fetch_agg_trades(
            symbol="BTCUSDT",
            start_ms=1_000,
            end_ms=5_000,
            rest_base="https://api.binance.com",
            limit=1000,
        )

    assert len(trades) == 1001
    assert mock_client.get.call_count == 2
    second_call_params = mock_client.get.call_args_list[1].kwargs["params"]
    assert second_call_params["startTime"] == 2_000


def test_fetch_agg_trades_page_uses_client_when_provided() -> None:
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = [{"a": 1, "p": "1", "q": "1", "T": 100, "m": False}]
    mock_client.get.return_value = mock_response

    page = fetch_agg_trades_page(
        symbol="ETHUSDT",
        start_ms=100,
        end_ms=200,
        rest_base="https://api.binance.com",
        client=mock_client,
    )

    assert len(page) == 1
    mock_client.get.assert_called_once()
