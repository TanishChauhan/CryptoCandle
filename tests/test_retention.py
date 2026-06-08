"""Tests for shared.retention helpers."""

from __future__ import annotations

from shared.retention import purge_old_candles


def test_purge_old_candles_importable() -> None:
    assert callable(purge_old_candles)
