"""Scheduled retention runner (summarize + purge old 1m candles)."""

from __future__ import annotations

import os
import sys
import time

import structlog

from shared.retention import purge_old_candles

log = structlog.get_logger("data-retention")


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "true" if default else "false").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _run_once() -> None:
    keep_days = int(os.getenv("RETENTION_KEEP_DAYS", "1"))
    summarize_first = _env_bool("RETENTION_SUMMARIZE_FIRST", True)
    metrics_keep_days = int(os.getenv("RETENTION_METRICS_KEEP_DAYS", "7"))

    result = purge_old_candles(
        keep_days=keep_days,
        summarize_first=summarize_first,
        metrics_keep_days=metrics_keep_days,
    )
    log.info(
        "retention_complete",
        cutoff=result.cutoff_iso,
        daily_summary_rows=result.daily_summary_rows,
        candles_deleted=result.candles_deleted,
        metrics_deleted=result.metrics_deleted,
        keep_days=keep_days,
        summarize_first=summarize_first,
    )


def main() -> None:
    once = "--once" in sys.argv or os.getenv("RETENTION_RUN_ONCE", "").lower() in {"1", "true", "yes"}
    interval_hours = max(1, int(os.getenv("RETENTION_INTERVAL_HOURS", "24")))

    if once:
        _run_once()
        return

    log.info("retention_scheduler_started", interval_hours=interval_hours)
    while True:
        try:
            _run_once()
        except Exception as exc:
            log.exception("retention_failed", error=str(exc))
        time.sleep(interval_hours * 3600)


if __name__ == "__main__":
    main()
