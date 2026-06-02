"""Pure data quality evaluation helpers (no DB dependency)."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class DlqQualityResult:
    records_in: int
    records_dlq: int
    dlq_ratio: float
    status: str
    message: str


def compute_dlq_ratio(*, records_in: int, records_dlq: int) -> float:
    if records_in <= 0:
        return 0.0
    return records_dlq / records_in


def evaluate_dlq_ratio(dlq_ratio: float) -> tuple[str, str]:
    warn_ratio = float(os.getenv("DLQ_WARN_RATIO", "0.01"))
    fail_ratio = float(os.getenv("DLQ_FAIL_RATIO", "0.05"))

    if dlq_ratio >= fail_ratio:
        return (
            "fail",
            f"DLQ ratio {dlq_ratio:.4f} exceeded fail threshold {fail_ratio:.4f}",
        )
    if dlq_ratio >= warn_ratio:
        return (
            "warn",
            f"DLQ ratio {dlq_ratio:.4f} exceeded warn threshold {warn_ratio:.4f}",
        )
    return ("ok", f"DLQ ratio {dlq_ratio:.4f} within thresholds")
