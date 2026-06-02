from shared.watermark import normalize_watermark_interval


def test_watermark_numeric_only_gets_minutes_suffix() -> None:
    assert normalize_watermark_interval("10") == "10 minutes"


def test_watermark_already_has_unit_is_unchanged() -> None:
    assert normalize_watermark_interval("10 minutes") == "10 minutes"


def test_watermark_empty_defaults_to_ten_minutes() -> None:
    assert normalize_watermark_interval("") == "10 minutes"
