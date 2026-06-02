from __future__ import annotations

from shared.dlq import build_dlq_envelope
from shared.validation import ValidationErrorCode, validate_trade


def test_valid_trade_passes(sample_trade: dict) -> None:
    result = validate_trade(sample_trade, now_ms=1710000001000)
    assert result.is_valid is True
    assert result.event is not None
    assert result.issue is None


def test_null_price_rejected(sample_trade: dict) -> None:
    sample_trade["price"] = ""
    result = validate_trade(sample_trade, now_ms=1710000001000)
    assert result.is_valid is False
    assert result.issue is not None
    assert result.issue.code == ValidationErrorCode.NULL_PRICE


def test_negative_quantity_rejected(sample_trade: dict) -> None:
    sample_trade["quantity"] = "-0.10"
    result = validate_trade(sample_trade, now_ms=1710000001000)
    assert result.is_valid is False
    assert result.issue is not None
    assert result.issue.code == ValidationErrorCode.NEGATIVE_QUANTITY


def test_invalid_timestamp_rejected(sample_trade: dict) -> None:
    sample_trade["trade_time_ms"] = 1600000000000
    result = validate_trade(sample_trade, now_ms=1710000001000)
    assert result.is_valid is False
    assert result.issue is not None
    assert result.issue.code == ValidationErrorCode.INVALID_TIMESTAMP


def test_unknown_symbol_rejected(sample_trade: dict) -> None:
    sample_trade["symbol"] = "DOGEUSDT"
    result = validate_trade(sample_trade, now_ms=1710000001000)
    assert result.is_valid is False
    assert result.issue is not None
    assert result.issue.code == ValidationErrorCode.INVALID_SYMBOL


def test_non_dict_payload_marked_malformed() -> None:
    result = validate_trade("not-a-dict", now_ms=1710000001000)
    assert result.is_valid is False
    assert result.issue is not None
    assert result.issue.code == ValidationErrorCode.MALFORMED_JSON


def test_dlq_envelope_contains_error_context(sample_trade: dict) -> None:
    sample_trade["quantity"] = "-1"
    result = validate_trade(sample_trade, now_ms=1710000001000)
    assert result.issue is not None
    envelope = build_dlq_envelope(
        original_payload=sample_trade,
        issue=result.issue,
        stage="producer",
        symbol=sample_trade["symbol"],
        failed_at_ms=1710000001100,
    )

    assert envelope["error_code"] == ValidationErrorCode.NEGATIVE_QUANTITY
    assert envelope["stage"] == "producer"
    assert envelope["failed_at_ms"] == 1710000001100
