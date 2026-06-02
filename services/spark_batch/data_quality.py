"""Data quality checks for batch/Airflow workflows."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from shared.dq_quality import DlqQualityResult, compute_dlq_ratio, evaluate_dlq_ratio


def fetch_recent_pipeline_totals(*, lookback_hours: int = 24) -> tuple[int, int]:
    from services.spark_streaming.pg_writer import connect_postgres, load_postgres_config

    since = datetime.now(UTC) - timedelta(hours=lookback_hours)
    sql = """
    SELECT
      COALESCE(SUM(records_in), 0) AS records_in,
      COALESCE(SUM(records_dlq), 0) AS records_dlq
    FROM analytics.pipeline_metrics
    WHERE recorded_at >= %s;
    """

    with connect_postgres(load_postgres_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (since,))
            row = cur.fetchone()

    if not row:
        return 0, 0
    return int(row[0]), int(row[1])


def run_dlq_quality_check(*, lookback_hours: int = 24) -> DlqQualityResult:
    records_in, records_dlq = fetch_recent_pipeline_totals(lookback_hours=lookback_hours)
    dlq_ratio = compute_dlq_ratio(records_in=records_in, records_dlq=records_dlq)
    status, message = evaluate_dlq_ratio(dlq_ratio)
    return DlqQualityResult(
        records_in=records_in,
        records_dlq=records_dlq,
        dlq_ratio=dlq_ratio,
        status=status,
        message=message,
    )


def assert_dlq_quality(*, lookback_hours: int = 24) -> None:
    result = run_dlq_quality_check(lookback_hours=lookback_hours)
    if result.status == "fail":
        raise RuntimeError(result.message)
    if result.status == "warn":
        print(f"WARNING: {result.message}")
