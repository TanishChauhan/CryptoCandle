"""Shared pipeline contracts and validation helpers."""

# Avoid eager imports so lightweight consumers (e.g. dashboard) need only their deps.
__all__ = [
    "TradeEvent",
    "ValidationErrorCode",
    "ValidationIssue",
    "ValidationResult",
    "validate_trade",
    "build_dlq_envelope",
]
