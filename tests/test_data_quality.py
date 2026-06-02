from shared.dq_quality import compute_dlq_ratio, evaluate_dlq_ratio


def test_compute_dlq_ratio_zero_when_no_input() -> None:
    assert compute_dlq_ratio(records_in=0, records_dlq=5) == 0.0


def test_compute_dlq_ratio_basic() -> None:
    assert compute_dlq_ratio(records_in=100, records_dlq=3) == 0.03


def test_evaluate_dlq_ratio_ok(monkeypatch) -> None:
    monkeypatch.setenv("DLQ_WARN_RATIO", "0.01")
    monkeypatch.setenv("DLQ_FAIL_RATIO", "0.05")
    status, _ = evaluate_dlq_ratio(0.005)
    assert status == "ok"


def test_evaluate_dlq_ratio_warn(monkeypatch) -> None:
    monkeypatch.setenv("DLQ_WARN_RATIO", "0.01")
    monkeypatch.setenv("DLQ_FAIL_RATIO", "0.05")
    status, message = evaluate_dlq_ratio(0.02)
    assert status == "warn"
    assert "warn threshold" in message


def test_evaluate_dlq_ratio_fail(monkeypatch) -> None:
    monkeypatch.setenv("DLQ_WARN_RATIO", "0.01")
    monkeypatch.setenv("DLQ_FAIL_RATIO", "0.05")
    status, message = evaluate_dlq_ratio(0.10)
    assert status == "fail"
    assert "fail threshold" in message
