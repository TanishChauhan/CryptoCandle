"""Spark streaming interval helpers (no PySpark dependency)."""

from __future__ import annotations

import re

_DURATION_SUFFIXES = frozenset({"second", "seconds", "minute", "minutes", "hour", "hours", "day", "days"})


def normalize_watermark_interval(raw: str) -> str:
    """Normalize WATERMARK_MINUTES env values for Spark withWatermark (e.g. '10' -> '10 minutes')."""
    value = raw.strip()
    if not value:
        return "10 minutes"
    parts = value.split()
    if len(parts) == 1 and re.fullmatch(r"\d+", parts[0]):
        return f"{parts[0]} minutes"
    if parts[-1].lower() not in _DURATION_SUFFIXES:
        return f"{value} minutes"
    return value
