"""Dead-letter queue envelope helpers."""

from __future__ import annotations

from time import time
from typing import Any

from shared.validation import ValidationIssue


def build_dlq_envelope(
    *,
    original_payload: Any,
    issue: ValidationIssue,
    stage: str,
    symbol: str | None = None,
    failed_at_ms: int | None = None,
) -> dict[str, Any]:
    """Build a consistent DLQ record for rejected events."""
    return {
        "original_payload": original_payload,
        "error_code": issue.code,
        "error_message": issue.message,
        "field": issue.field,
        "failed_at_ms": failed_at_ms if failed_at_ms is not None else int(time() * 1000),
        "stage": stage,
        "symbol": symbol,
    }
