"""Spark Structured Streaming job: Kafka -> validate/dedup/watermark -> 1m OHLC."""



from __future__ import annotations



import json

import os

import time

from pathlib import Path



from pyspark.sql import SparkSession

from pyspark.sql import functions as F



from services.spark_streaming.pg_writer import insert_pipeline_metrics, upsert_candles_1m

from services.spark_streaming.sinks import enrich_invalid_for_dlq, write_raw_trades_parquet

from services.spark_streaming.transforms import aggregate_1m_ohlc, dedup_and_watermark, parse_kafka_trades, validate_and_enrich

from shared.watermark import normalize_watermark_interval



_DEBUG_LOG_PATHS = (

    Path("/data/debug-f8fbea.log"),

    Path(__file__).resolve().parents[2] / "debug-f8fbea.log",

)





def _debug_log(hypothesis_id: str, location: str, message: str, data: dict) -> None:

    # #region agent log

    payload = {

        "sessionId": "f8fbea",

        "runId": os.getenv("DEBUG_RUN_ID", "post-fix"),

        "hypothesisId": hypothesis_id,

        "location": location,

        "message": message,

        "data": data,

        "timestamp": int(time.time() * 1000),

    }

    line = json.dumps(payload, default=str) + "\n"

    for path in _DEBUG_LOG_PATHS:

        try:

            path.parent.mkdir(parents=True, exist_ok=True)

            with path.open("a", encoding="utf-8") as fh:

                fh.write(line)

            break

        except OSError:

            continue

    # #endregion





def load_symbols() -> list[str]:

    raw = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT")

    return [s.strip().upper() for s in raw.split(",") if s.strip()]





def resolve_late_after_ms() -> int:

    """DLQ routing threshold for stale trades (separate from window watermark)."""

    raw = os.getenv("LATE_EVENT_AFTER_MINUTES", "30").strip()

    minutes = int(raw.split()[0]) if raw else 30

    return max(1, minutes) * 60 * 1000





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

        .option("failOnDataLoss", "false")

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

    late_after_ms = resolve_late_after_ms()



    allowed_symbols = load_symbols()



    _debug_log(

        "H2-H3",

        "stream_job.py:main",

        "stream_job_start",

        {

            "watermark_minutes": watermark_minutes,

            "late_after_ms": late_after_ms,

            "starting_offsets": starting_offsets,

            "trigger_seconds": trigger_seconds,

        },

    )



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



    df_validated = validate_and_enrich(

        df_parsed,

        allowed_symbols=allowed_symbols,

        max_time_skew_ms=max_time_skew_ms,

        late_after_ms=late_after_ms,

    )



    df_valid = df_validated.filter(F.col("is_valid") == F.lit(True))

    df_dedup = dedup_and_watermark(df_valid, watermark_minutes=watermark_minutes)

    df_agg = aggregate_1m_ohlc(df_dedup)



    # --- Combined validated sink: raw Parquet + DLQ (single Kafka consumer) ---

    validated_checkpoint = os.path.join(base_checkpoint_dir, "validated_trades")



    def foreach_batch_validated(batch_df, batch_id: int) -> None:

        t0 = time.monotonic()

        valid_df = batch_df.filter(F.col("is_valid") == F.lit(True))

        invalid_df = batch_df.filter(F.col("is_valid") == F.lit(False))



        records_valid = valid_df.count()

        records_dlq = invalid_df.count()

        records_in = records_valid + records_dlq



        late_dlq = 0

        if records_dlq > 0:

            late_dlq = invalid_df.filter(F.col("error_code") == F.lit("LATE_EVENT")).count()



        if records_valid > 0:

            write_raw_trades_parquet(valid_df, raw_base_dir=raw_data_dir)



        if records_dlq > 0:

            dlq_df = enrich_invalid_for_dlq(invalid_df, stage="spark")

            (

                dlq_df.write.format("kafka")

                .option("kafka.bootstrap.servers", bootstrap_servers)

                .option("topic", dlq_topic)

                .save()

            )



        batch_duration_ms = int((time.monotonic() - t0) * 1000)

        _debug_log(

            "H3",

            "stream_job.py:foreach_batch_validated",

            "validated_batch_complete",

            {

                "batch_id": batch_id,

                "records_in": records_in,

                "records_valid": records_valid,

                "records_dlq": records_dlq,

                "late_event_dlq": late_dlq,

                "batch_duration_ms": batch_duration_ms,

            },

        )

        insert_pipeline_metrics(

            job_name="spark_validated_trades",

            kafka_lag=None,

            batch_duration_ms=batch_duration_ms,

            records_in=records_in,

            records_out=records_valid,

            records_valid=records_valid,

            records_dlq=records_dlq,

            status="ok",

        )



    (

        df_validated.writeStream.foreachBatch(foreach_batch_validated)

        .outputMode("append")

        .option("checkpointLocation", validated_checkpoint)

        .trigger(processingTime=f"{trigger_seconds} seconds")

        .start()

    )



    # --- Candles sink (PostgreSQL) ---

    def foreach_batch_valid(batch_df, batch_id: int) -> None:

        t0 = time.monotonic()

        records_out = batch_df.count()

        records_in = records_out

        records_valid = records_out



        if records_out > 0:

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

        _debug_log(

            "H2",

            "stream_job.py:foreach_batch_valid",

            "candles_batch_complete",

            {

                "batch_id": batch_id,

                "records_out": records_out,

                "batch_duration_ms": batch_duration_ms,

            },

        )

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



    spark.streams.awaitAnyTermination()





if __name__ == "__main__":

    main()


