"""Spark Structured Streaming job: Kafka -> validate/dedup/watermark -> 1m OHLC."""

from __future__ import annotations

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from services.spark_streaming.pg_writer import insert_pipeline_metrics, upsert_candles_1m
from services.spark_streaming.sinks import enrich_invalid_for_dlq, write_raw_trades_parquet
from services.spark_streaming.transforms import aggregate_1m_ohlc, dedup_and_watermark, parse_kafka_trades, validate_and_enrich
from shared.watermark import normalize_watermark_interval


def load_symbols() -> list[str]:
    raw = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT")
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_kafka_trades(spark: SparkSession, *, bootstrap_servers: str, topic: str, starting_offsets: str):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )


def main() -> None:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    trades_topic = os.getenv("KAFKA_TOPIC_TRADES", "crypto_trades")
    dlq_topic = os.getenv("KAFKA_TOPIC_DLQ", "dead_letter_queue")

    raw_data_dir = os.getenv("RAW_DATA_DIR", "/data/raw/trades")
    base_checkpoint_dir = os.getenv("SPARK_CHECKPOINT_DIR", "/data/checkpoints/stream_trades")

    watermark_minutes = normalize_watermark_interval(os.getenv("WATERMARK_MINUTES", "10 minutes"))
    trigger_seconds = int(os.getenv("STREAM_TRIGGER_SECONDS", "5"))
    starting_offsets = os.getenv("STARTING_OFFSETS", "latest")
    max_time_skew_ms = int(os.getenv("MAX_TIME_SKEW_MS", str(24 * 60 * 60 * 1000)))

    allowed_symbols = load_symbols()

    # Postgres tables are created by infra/postgres/init.sql.
    app_name = "crypto-spark-streaming"
    spark = build_spark(app_name)
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    df_kafka = read_kafka_trades(
        spark,
        bootstrap_servers=bootstrap_servers,
        topic=trades_topic,
        starting_offsets=starting_offsets,
    )

    df_parsed = parse_kafka_trades(df_kafka)
    late_minutes = int(watermark_minutes.split()[0])
    late_after_ms = late_minutes * 60 * 1000

    df_validated = validate_and_enrich(
        df_parsed,
        allowed_symbols=allowed_symbols,
        max_time_skew_ms=max_time_skew_ms,
        late_after_ms=late_after_ms,
    )

    df_valid = df_validated.filter(F.col("is_valid") == F.lit(True))
    df_invalid = df_validated.filter(F.col("is_valid") == F.lit(False))

    # Raw zone wants *incoming valid trades*, not aggregated candles.
    # We therefore sink df_valid before stateful dedup/watermark.
    df_dedup = dedup_and_watermark(df_valid, watermark_minutes=watermark_minutes)
    df_agg = aggregate_1m_ohlc(df_dedup)

    # --- Raw trade sink (Parquet) ---
    raw_checkpoint = os.path.join(base_checkpoint_dir, "raw_valid_trades")

    def foreach_batch_raw(batch_df, batch_id: int) -> None:
        t0 = time.monotonic()
        records_in = batch_df.count()
        if records_in > 0:
            write_raw_trades_parquet(batch_df, raw_base_dir=raw_data_dir)
        batch_duration_ms = int((time.monotonic() - t0) * 1000)
        insert_pipeline_metrics(
            job_name="spark_raw_valid_trades",
            kafka_lag=None,
            batch_duration_ms=batch_duration_ms,
            records_in=records_in,
            records_out=records_in,
            records_valid=records_in,
            records_dlq=0,
            status="ok",
        )

    (
        df_valid.writeStream.foreachBatch(foreach_batch_raw)
        .outputMode("append")
        .option("checkpointLocation", raw_checkpoint)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )

    # --- Valid path sink (PostgreSQL candles) ---
    def foreach_batch_valid(batch_df, batch_id: int) -> None:
        t0 = time.monotonic()
        # batch_df is already aggregated (static DF in foreachBatch).
        records_out = batch_df.count()
        records_in = records_out  # we only have aggregated counts at this stage
        records_valid = records_out

        if records_out > 0:
            # Postgres upsert expects collected rows; for MVP this is OK because aggregation output is small.
            rows = [
                (
                    r["symbol"],
                    r["window_start"],
                    r["window_end"],
                    r["open_price"],
                    r["avg_price"],
                    r["high_price"],
                    r["low_price"],
                    r["close_price"],
                    r["volume"],
                    r["quote_volume"],
                    r["trade_count"],
                    r["volatility"],
                )
                for r in batch_df.collect()
            ]
            upsert_candles_1m(rows)

        batch_duration_ms = int((time.monotonic() - t0) * 1000)
        insert_pipeline_metrics(
            job_name="spark_valid_stream",
            kafka_lag=None,
            batch_duration_ms=batch_duration_ms,
            records_in=records_in,
            records_out=records_out,
            records_valid=records_valid,
            records_dlq=0,
            status="ok",
        )

    valid_checkpoint = os.path.join(base_checkpoint_dir, "valid")
    (
        df_agg.writeStream.foreachBatch(foreach_batch_valid)
        .outputMode("update")
        .option("checkpointLocation", valid_checkpoint)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )

    # --- Invalid path sink (DLQ Kafka + metrics) ---
    df_invalid_dlq = enrich_invalid_for_dlq(df_invalid, stage="spark")

    def foreach_batch_invalid(batch_df, batch_id: int) -> None:
        t0 = time.monotonic()
        records_dlq = batch_df.count()
        if records_dlq > 0:
            # Batch DF -> Kafka sink
            (
                batch_df.write.format("kafka")
                .option("kafka.bootstrap.servers", bootstrap_servers)
                .option("topic", dlq_topic)
                .save()
            )
        batch_duration_ms = int((time.monotonic() - t0) * 1000)
        insert_pipeline_metrics(
            job_name="spark_invalid_stream",
            kafka_lag=None,
            batch_duration_ms=batch_duration_ms,
            records_in=records_dlq,
            records_out=records_dlq,
            records_valid=0,
            records_dlq=records_dlq,
            status="ok",
        )

    invalid_checkpoint = os.path.join(base_checkpoint_dir, "invalid")
    (
        df_invalid_dlq.writeStream.foreachBatch(foreach_batch_invalid)
        .outputMode("append")
        .option("checkpointLocation", invalid_checkpoint)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

