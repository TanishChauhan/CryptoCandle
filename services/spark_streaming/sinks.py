"""Parquet and Kafka sinks helpers for streaming micro-batches."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


def write_raw_trades_parquet(batch_df: DataFrame, *, raw_base_dir: str) -> None:
    """Append validated trade rows to partitioned Parquet (by trade_time)."""
    df = (
        batch_df.withColumn("year", F.year(F.col("trade_time")))
        .withColumn("month", F.month(F.col("trade_time")))
        .withColumn("day", F.dayofmonth(F.col("trade_time")))
        .withColumn("hour", F.hour(F.col("trade_time")))
    )

    (
        df.write.mode("append")
        .partitionBy("year", "month", "day", "hour")
        .parquet(raw_base_dir)
    )


def enrich_invalid_for_dlq(df_invalid: DataFrame, *, stage: str) -> DataFrame:
    """
    Convert invalid rows into DLQ Kafka envelopes:
      value = JSON string
      key = symbol
    """
    failed_at_ms = (F.unix_timestamp(F.current_timestamp()) * F.lit(1000)).cast("long")

    envelope = F.struct(
        F.col("raw_value").alias("original_payload"),
        F.col("error_code").alias("error_code"),
        F.col("error_message").alias("error_message"),
        F.lit(None).cast("string").alias("field"),
        failed_at_ms.alias("failed_at_ms"),
        F.lit(stage).alias("stage"),
        F.col("symbol").alias("symbol"),
    )

    return (
        df_invalid.select(
            F.col("symbol").alias("key"),
            F.to_json(envelope).alias("value"),
        )
    )


def ensure_int_columns(df: DataFrame, cols: list[str]) -> DataFrame:
    out = df
    for c in cols:
        out = out.withColumn(c, F.col(c).cast(IntegerType()))
    return out

