"""PySpark DataFrame transforms for trade validation and 1-minute OHLC."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DecimalType, LongType, StringType, StructField, StructType


def kafka_trade_value_schema() -> StructType:
    """Schema for Kafka 'crypto_trades' message value (JSON)."""
    # NOTE: price/quantity are strings in transit (producer emits string values).
    return StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("trade_id", LongType(), True),
            StructField("price", StringType(), True),
            StructField("quantity", StringType(), True),
            StructField("quote_qty", StringType(), True),
            StructField("trade_time_ms", LongType(), True),
            StructField("is_buyer_maker", BooleanType(), True),
            StructField("ingested_at_ms", LongType(), True),
            StructField("source", StringType(), True),
        ]
    )


def parse_kafka_trades(df_kafka: DataFrame) -> DataFrame:
    """Parse Kafka messages into a normalized flat schema."""
    schema = kafka_trade_value_schema()
    # Kafka source: df_kafka.value is binary. Cast to string for parsing + DLQ audit.
    return (
        df_kafka.withColumn("raw_value", F.col("value").cast("string"))
        .withColumn("data", F.from_json(F.col("raw_value"), schema))
        .select(
            F.col("raw_value"),
            F.col("data.*"),
        )
    )


def validate_and_enrich(
    df_trades: DataFrame,
    *,
    allowed_symbols: list[str],
    max_time_skew_ms: int,
    late_after_ms: int | None,
) -> DataFrame:
    """
    Add validation columns (is_valid/error_code/error_message) using DataFrame expressions only.

    This function must not use spark.sql strings; it is meant to be fully DataFrame API testable.
    """
    allowed_set = list(allowed_symbols)
    now_ms = (F.unix_timestamp(F.current_timestamp()) * F.lit(1000)).cast("long")

    # Cast numeric-like strings. Invalid casts become null.
    price_dec = F.col("price").cast(DecimalType(20, 8))
    qty_dec = F.col("quantity").cast(DecimalType(38, 12))
    quote_qty_dec = F.col("quote_qty").cast(DecimalType(38, 12))

    trade_time_ms = F.col("trade_time_ms").cast("long")
    trade_time = F.to_timestamp(trade_time_ms / F.lit(1000.0))

    price_is_null = F.col("price").isNull() | (F.length(F.trim(F.col("price"))) == 0)
    qty_is_null = F.col("quantity").isNull() | (F.length(F.trim(F.col("quantity"))) == 0)
    symbol_is_valid = F.col("symbol").isin(allowed_set)

    # Cast validity
    price_cast_invalid = price_dec.isNull() & (~price_is_null)
    qty_cast_invalid = qty_dec.isNull() & (~qty_is_null)

    negative_qty = qty_dec < F.lit(0)

    # Timestamp bounds: within +/- max_time_skew_ms around 'now'
    invalid_ts = (
        trade_time_ms.isNull()
        | (trade_time_ms <= 0)
        | (trade_time_ms < (now_ms - F.lit(max_time_skew_ms)))
        | (trade_time_ms > (now_ms + F.lit(max_time_skew_ms)))
    )

    # Late events: older than the allowed watermark lateness threshold.
    # We compute this explicitly so late events can be routed to the DLQ.
    is_late = (
        F.lit(False)
        if late_after_ms is None
        else trade_time_ms < (now_ms - F.lit(late_after_ms))
    )

    # Compute error_code with a stable priority order.
    # If multiple fail, the first WHEN wins.
    error_code = (
        F.when(price_is_null, F.lit("NULL_PRICE"))
        .when(price_cast_invalid | (price_dec <= F.lit(0)), F.lit("INVALID_PRICE"))
        .when(qty_is_null, F.lit("NULL_QUANTITY"))
        .when(qty_cast_invalid | negative_qty, F.lit("NEGATIVE_QUANTITY"))
        .when(invalid_ts, F.lit("INVALID_TIMESTAMP"))
        .when(is_late, F.lit("LATE_EVENT"))
        .when(~symbol_is_valid, F.lit("INVALID_SYMBOL"))
        .otherwise(F.lit(None).cast(StringType()))
    )

    error_message = (
        F.when(price_is_null, F.lit("price cannot be null"))
        .when(price_cast_invalid | (price_dec <= F.lit(0)), F.lit("price must be numeric and > 0"))
        .when(qty_is_null, F.lit("quantity cannot be null"))
        .when(qty_cast_invalid | negative_qty, F.lit("quantity must be numeric and >= 0"))
        .when(invalid_ts, F.lit("trade_time_ms outside accepted time bounds"))
        .when(is_late, F.lit("event is late beyond watermark threshold"))
        .when(~symbol_is_valid, F.lit("symbol is not in allowed set"))
        .otherwise(F.lit(None).cast(StringType()))
    )

    is_valid = error_code.isNull()

    return (
        df_trades.withColumn("price_dec", price_dec)
        .withColumn("quantity_dec", qty_dec)
        .withColumn("quote_qty_dec", quote_qty_dec)
        .withColumn("trade_time_ms", trade_time_ms)
        .withColumn("trade_time", trade_time)
        .withColumn("now_ms", now_ms)
        .withColumn("is_valid", is_valid)
        .withColumn("error_code", error_code)
        .withColumn("error_message", error_message)
    )


def dedup_and_watermark(valid_df: DataFrame, *, watermark_minutes: str) -> DataFrame:
    """Drop duplicates by event_id and apply watermark using trade_time."""
    return (
        valid_df.withWatermark("trade_time", watermark_minutes)
        .dropDuplicates(["event_id"])
    )


def aggregate_1m_ohlc(valid_dedup_df: DataFrame) -> DataFrame:
    """Compute 1-minute OHLC + volume + volatility proxy per symbol.

    Semantics are mirrored by ``shared.aggregation_ref.compute_1m_candles`` for unit tests.
    """
    w = F.window(F.col("trade_time"), "1 minute")
    # Use min_by/max_by for deterministic open/close by event timestamp.
    return (
        valid_dedup_df.groupBy(F.col("symbol"), w.alias("win"))
        .agg(
            F.min_by(F.col("price_dec"), F.col("trade_time")).alias("open_price"),
            F.avg(F.col("price_dec")).alias("avg_price"),
            F.max(F.col("price_dec")).alias("high_price"),
            F.min(F.col("price_dec")).alias("low_price"),
            F.max_by(F.col("price_dec"), F.col("trade_time")).alias("close_price"),
            F.sum(F.col("quantity_dec")).alias("volume"),
            F.sum(F.col("quote_qty_dec")).alias("quote_volume"),
            F.count(F.lit(1)).alias("trade_count"),
            F.stddev_samp(F.col("price_dec")).alias("volatility"),
        )
        .select(
            F.col("symbol"),
            F.col("win.start").alias("window_start"),
            F.col("win.end").alias("window_end"),
            "open_price",
            "avg_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "quote_volume",
            "trade_count",
            "volatility",
        )
    )

