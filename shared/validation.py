"""Shared validation rules for trade events."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from time import time
from typing import Any

from pydantic import ValidationError

from shared.schema import TradeEvent

DEFAULT_ALLOWED_SYMBOLS = {"BTCUSDT", "ETHUSDT", "SOLUSDT"}
DEFAULT_TIME_SKEW_MS = 24 * 60 * 60 * 1000


class ValidationErrorCode(StrEnum):
    MALFORMED_JSON = "MALFORMED_JSON"
    SCHEMA_VALIDATION_FAILED = "SCHEMA_VALIDATION_FAILED"
    NULL_PRICE = "NULL_PRICE"
    INVALID_PRICE = "INVALID_PRICE"
    NULL_QUANTITY = "NULL_QUANTITY"
    NEGATIVE_QUANTITY = "NEGATIVE_QUANTITY"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    INVALID_SYMBOL = "INVALID_SYMBOL"


@dataclass(frozen=True)
class ValidationIssue:
    code: ValidationErrorCode
    message: str
    field: str | None = None


@dataclass(frozen=True)
class ValidationResult:
    is_valid: bool
    event: TradeEvent | None = None
    issue: ValidationIssue | None = None


def _parse_positive_decimal(value: str, *, field_name: str) -> ValidationIssue | None:
    if value is None or str(value).strip() == "":
        null_code = (
            ValidationErrorCode.NULL_PRICE
            if field_name == "price"
            else ValidationErrorCode.NULL_QUANTITY
        )
        return ValidationIssue(code=null_code, message=f"{field_name} cannot be null", field=field_name)

    try:
        numeric = Decimal(str(value))
    except (InvalidOperation, TypeError):
        code = (
            ValidationErrorCode.INVALID_PRICE
            if field_name == "price"
            else ValidationErrorCode.NEGATIVE_QUANTITY
        )
        return ValidationIssue(code=code, message=f"{field_name} must be numeric", field=field_name)

    if field_name == "price" and numeric <= 0:
        return ValidationIssue(
            code=ValidationErrorCode.INVALID_PRICE,
            message="price must be > 0",
            field=field_name,
        )
    if field_name == "quantity" and numeric < 0:
        return ValidationIssue(
            code=ValidationErrorCode.NEGATIVE_QUANTITY,
            message="quantity must be >= 0",
            field=field_name,
        )

    return None


def validate_trade(
    payload: Any,
    *,
    now_ms: int | None = None,
    allowed_symbols: set[str] | None = None,
    max_time_skew_ms: int = DEFAULT_TIME_SKEW_MS,
) -> ValidationResult:
    """Validate a trade payload without raising for expected bad records."""
    if not isinstance(payload, dict):
        return ValidationResult(
            is_valid=False,
            issue=ValidationIssue(
                code=ValidationErrorCode.MALFORMED_JSON,
                message="payload must be an object",
            ),
        )

    try:
        event = TradeEvent.model_validate(payload)
    except ValidationError as exc:
        return ValidationResult(
            is_valid=False,
            issue=ValidationIssue(
                code=ValidationErrorCode.SCHEMA_VALIDATION_FAILED,
                message=exc.errors()[0]["msg"] if exc.errors() else "schema validation failed",
            ),
        )

    issue = _parse_positive_decimal(event.price, field_name="price")
    if issue:
        return ValidationResult(is_valid=False, issue=issue)

    issue = _parse_positive_decimal(event.quantity, field_name="quantity")
    if issue:
        return ValidationResult(is_valid=False, issue=issue)

    symbols = allowed_symbols or DEFAULT_ALLOWED_SYMBOLS
    if event.symbol not in symbols:
        return ValidationResult(
            is_valid=False,
            issue=ValidationIssue(
                code=ValidationErrorCode.INVALID_SYMBOL,
                message=f"symbol must be one of {sorted(symbols)}",
                field="symbol",
            ),
        )

    current_ms = now_ms if now_ms is not None else int(time() * 1000)
    if (
        event.trade_time_ms <= 0
        or event.trade_time_ms < current_ms - max_time_skew_ms
        or event.trade_time_ms > current_ms + max_time_skew_ms
    ):
        return ValidationResult(
            is_valid=False,
            issue=ValidationIssue(
                code=ValidationErrorCode.INVALID_TIMESTAMP,
                message="trade_time_ms is outside accepted time bounds",
                field="trade_time_ms",
            ),
        )

    return ValidationResult(is_valid=True, event=event)
