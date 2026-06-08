# Concepts in This Project

A beginner-friendly deep dive into **every important data engineering concept** used in this repository. Each section explains what it is, why it matters here, and where to see it in the code.

---

## 1. ETL (Extract, Transform, Load)

### What is it?

The foundational pattern of data engineering:

| Step | Meaning | In this project |
|------|---------|-----------------|
| **Extract** | Get data from a source | Binance WebSocket + REST API |
| **Transform** | Clean, validate, aggregate | Validation, dedup, OHLC aggregation |
| **Load** | Store results somewhere useful | Parquet, PostgreSQL, Kafka DLQ |

### Why it matters

Almost every data pipeline is ETL (or its variant ELT). This project is a complete ETL system you can point to in interviews.

---

## 2. Streaming vs Batch Processing

### Streaming (real-time)

Data is processed **continuously** as it arrives. Low latency (seconds).

**In this project:** Spark Structured Streaming reads Kafka every 5 seconds and produces candles.

### Batch (scheduled)

Data is processed in **large chunks** on a schedule (hourly, daily). Higher latency but more efficient for heavy computation.

**In this project:** Airflow triggers backfill (hourly), compaction + daily summary (daily).

### Hybrid (lambda architecture)

Use **both** streaming and batch:

| Layer | Handles | This project |
|-------|---------|--------------|
| Speed layer | Fresh data, low latency | Spark Streaming |
| Batch layer | Completeness, heavy compute | Airflow + PySpark batch |
| Serving layer | Queryable results | PostgreSQL |

**Why hybrid?** Streaming alone misses data during outages. Batch alone can't give live charts. Together you get freshness AND reliability.

---

## 3. Event-Driven Architecture

### What is it?

Services communicate by **passing events** (messages) rather than calling each other directly.

```text
Producer publishes event → Kafka holds it → Spark consumes it
```

Producer doesn't know Spark exists. Spark doesn't know about the dashboard. They communicate only through Kafka and PostgreSQL.

### Why it matters

- Services can be developed, deployed, and scaled independently
- If Spark crashes, Kafka buffers events
- New consumers can be added without changing the producer

**Where:** Kafka topics `crypto_trades` and `dead_letter_queue`.

---

## 4. Schema Validation & Data Contracts

### What is it?

A formal agreement about what data should look like before it enters the pipeline.

### In this project

| Artifact | Purpose |
|----------|---------|
| `schemas/trade_event.json` | JSON Schema (documentation + tooling) |
| `shared/schema.py` | Pydantic model (runtime enforcement) |
| `shared/validation.py` | Business rules (price > 0, valid symbol, etc.) |

### Why it matters

Without contracts, one service might send `{price: null}` and another crashes trying to compute OHLC. Contracts catch problems early.

### Fail-safe design

Validation **never raises exceptions** for bad data. It returns a `ValidationResult` with `is_valid=False`. The caller routes to DLQ. The pipeline keeps running.

---

## 5. Dead Letter Queue (DLQ)

### What is it?

A separate queue/topic for messages that **failed processing** but shouldn't be silently dropped.

### In this project

Topic: `dead_letter_queue`

DLQ envelope contains:
- `original_payload` — the bad record
- `error_code` — e.g. `NULL_PRICE`, `INVALID_SYMBOL`
- `error_message` — human-readable explanation
- `stage` — where rejection happened (`producer` or `spark`)
- `failed_at_ms` — timestamp

### Two enforcement points

1. **Producer** — rejects before Kafka main topic
2. **Spark** — second pass catches anything that slipped through

### Why it matters

In production, 0.1%–1% of events are often bad (upstream bugs, schema changes). DLQ lets you:
- Debug without stopping the pipeline
- Measure data quality over time
- Replay fixed records later

**Code:** `shared/dlq.py`, producer `main.py`, `sinks.enrich_invalid_for_dlq()`.

---

## 6. Deduplication

### What is it?

Ensuring the same event isn't counted twice.

### Why duplicates happen

- Kafka **at-least-once delivery** (messages may be redelivered)
- Producer reconnects and replays
- REST backfill overlaps with streaming data

### How this project handles it

| Stage | Method |
|-------|--------|
| Spark streaming | `dropDuplicates(["event_id"])` before aggregation |
| Batch compaction | `dropDuplicates(["event_id"])` |
| PostgreSQL candles | `ON CONFLICT DO UPDATE` (upsert, not insert) |

### event_id format

```text
{symbol}-{trade_id}-{trade_time_ms}
```

Example: `BTCUSDT-5123456789-1712345678901`

This is deterministic — the same trade always produces the same ID.

**Code:** `transforms.dedup_and_watermark()`, `shared/aggregation_ref.dedup_by_event_id()`.

---

## 7. Watermarking

### What is it?

A mechanism in stream processing to handle **late-arriving events** — records that show up after their time window should have closed.

### The problem

```text
Trade happens at 14:32:00
Network delay → arrives at 14:35:00
The 14:32 candle window already closed
```

Without watermarks, you'd either:
- Never close windows (unbounded state)
- Close windows and lose late data silently

### How watermarking works

```text
watermark = max_event_time - allowed_lateness

If event_time < watermark → event is "late"
```

### In this project

```python
.withWatermark("trade_time", "2 minutes")
```

Events arriving more than 2 minutes late are flagged as `LATE_EVENT` and routed to DLQ.

**Trade-off:** Shorter watermark = faster candles but more late rejections. Longer watermark = more complete candles but higher latency.

**Code:** `transforms.dedup_and_watermark()`, `shared/watermark.py`.

---

## 8. Windowed Aggregation

### What is it?

Grouping events by time windows and computing statistics.

### In this project: 1-minute OHLC candles

```text
All trades for BTCUSDT between 14:32:00 and 14:33:00
  → one candle row with open, high, low, close, volume
```

| Metric | Calculation |
|--------|-------------|
| Open | Price of earliest trade in window |
| High | Maximum price |
| Low | Minimum price |
| Close | Price of latest trade in window |
| Avg | Mean price |
| Volume | Sum of quantities |
| Volatility | Standard deviation of prices |
| Trade count | Number of trades |

### Spark implementation

```python
groupBy(symbol, window(trade_time, "1 minute")).agg(...)
```

Uses `min_by(price, trade_time)` for deterministic open and `max_by` for close.

**Code:** `transforms.aggregate_1m_ohlc()`, `shared/aggregation_ref.compute_1m_candles()`.

---

## 9. Micro-Batching

### What is it?

Structured Streaming doesn't process event-by-event. It collects events into **small batches** and processes each batch as a static DataFrame.

### In this project

```python
.trigger(processingTime="5 seconds")
```

Every 5 seconds, Spark:
1. Reads all new Kafka messages since last checkpoint
2. Runs transforms on the batch
3. Writes outputs
4. Commits checkpoint

### Why not pure event-by-event?

Micro-batching gives you Spark's full DataFrame API (groupBy, window, join) while still being "streaming." It's the best of both worlds for most use cases.

---

## 10. Checkpointing

### What is it?

Spark Structured Streaming saves its progress (Kafka offsets + computation state) to durable storage so it can **resume exactly where it left off** after a crash.

### In this project

```text
data/checkpoints/stream_trades/
  ├── raw_valid_trades/    ← raw Parquet stream checkpoint
  ├── valid/               ← candle aggregation checkpoint
  └── invalid/             ← DLQ stream checkpoint
```

Each of the three write streams has its **own checkpoint** — they progress independently.

### What happens on restart

1. Spark reads checkpoint → knows last Kafka offset
2. Resumes consuming from that offset
3. Re-processes any uncommitted batches
4. Dedup + upsert prevent duplicate output

**Never delete checkpoints** unless you want to reprocess from scratch.

---

## 11. Idempotent Writes

### What is it?

Writing the same data twice produces the **same result** as writing once. No duplicates, no corruption.

### In this project

| Sink | Idempotency mechanism |
|------|----------------------|
| PostgreSQL candles | `INSERT ... ON CONFLICT DO UPDATE` |
| PostgreSQL daily summary | `INSERT ... ON CONFLICT DO UPDATE` |
| Parquet raw | Append (duplicates possible; compaction dedups later) |
| Kafka DLQ | Append (duplicates acceptable for audit) |

### Why it matters

In distributed systems, retries are normal. Without idempotent writes, a retry creates duplicate candles with double volume.

---

## 12. Parquet & Data Lake Storage

### What is it?

**Parquet** is a columnar file format optimized for analytics. A **data lake** is storage that holds raw data in its native format until needed.

### In this project

```text
data/raw/trades/           ← "bronze" zone (raw valid trades)
data/raw/trades_compacted/ ← optimized for batch reads
```

### Partitioning

Files are organized by `year/month/day/hour` directories. When Spark reads "give me April 5 data," it only opens files in `year=2024/month=4/day=5/` — not the entire dataset.

### Why Parquet over CSV/JSON files?

| Advantage | Detail |
|-----------|--------|
| Compression | ~10x smaller than CSV |
| Columnar | Reading just `price` column is fast |
| Schema | Types embedded in file metadata |
| Splittable | Spark can parallelize reads |

---

## 13. Medallion Architecture (simplified)

### What is it?

A data organization pattern with quality tiers:

| Tier | Name | Quality | This project |
|------|------|---------|--------------|
| Raw | Bronze | As-ingested | `data/raw/trades/` |
| Cleaned | Silver | Validated, deduped | Compacted Parquet |
| Aggregated | Gold | Business-ready | `candles_1m`, `daily_summary` |

This project doesn't use bronze/silver/gold naming, but the pattern is the same.

---

## 14. Data Quality Gates

### What is it?

Automated checks that **fail the pipeline** when data quality degrades beyond acceptable thresholds.

### In this project

```text
DLQ ratio = records_dlq / records_in

< 1%  → ok
1–5%  → warn (log warning, continue)
≥ 5%  → fail (Airflow DAG fails, human investigates)
```

### Why not just DLQ?

DLQ handles individual bad records gracefully. But if 10% of all records are bad, something systemic is wrong (upstream outage, schema change). The quality gate catches that pattern.

**Code:** `shared/dq_quality.py`, `services/spark_batch/data_quality.py`, Airflow DAG `crypto_data_quality`.

---

## 15. Orchestration (Airflow)

### What is it?

Managing **when** and **how** batch jobs run — scheduling, dependencies, retries, monitoring.

### Key Airflow concepts

| Concept | Meaning | This project |
|---------|---------|--------------|
| DAG | Directed Acyclic Graph of tasks | 3 DAGs (backfill, daily, DQ) |
| Task | A unit of work | PythonOperator calling batch functions |
| Schedule | When to run | `@hourly`, `@daily` |
| Retry | Re-run on failure | 2 retries, 5 min delay |
| catchup | Run missed schedules | `false` (don't backfill past runs) |

### Why Airflow over cron?

- Retries with configurable delay
- Web UI showing run history
- Task dependencies (if you add more tasks later)
- Programmatic scheduling

---

## 16. Serving Layer

### What is it?

A database optimized for **reading** by applications, as opposed to writing by pipelines.

### In this project

PostgreSQL `analytics` schema is the serving layer:
- Dashboard reads from it
- Batch jobs write to it
- Streaming jobs upsert to it

### Upsert pattern

```sql
INSERT INTO candles_1m (...) VALUES (...)
ON CONFLICT (symbol, window_start) DO UPDATE SET ...
```

This means the serving layer always has the **latest** version of each candle, whether it came from streaming or was recomputed.

---

## 17. Observability & Pipeline Metrics

### What is it?

Tracking the health and performance of your pipeline in real time.

### In this project

Every micro-batch and batch job writes to `analytics.pipeline_metrics`:

| Metric | What it tells you |
|--------|-------------------|
| `records_in` / `records_out` | Throughput |
| `records_dlq` | Data quality |
| `batch_duration_ms` | Performance |
| `kafka_lag` | Consumer behind producer? |
| `status` | ok, skipped_no_data, fail |

The dashboard Pipeline Health tab visualizes these.

---

## 18. Fault Injection & Resilience Testing

### What is it?

Deliberately breaking things to prove the system recovers gracefully.

### The README fault-injection checklist

| Fault | Expected behavior |
|-------|-------------------|
| Empty price in Kafka | → DLQ, stream continues |
| Duplicate event_id | → dedup, no double volume |
| Stop producer 2 min | → Spark resumes, no PK violations |
| Stop spark-streaming 1 min | → Kafka buffers, lag recovers |
| Invalid symbol | → DLQ with `INVALID_SYMBOL` |
| High DLQ ratio | → Airflow DQ DAG fails |
| No raw data for daily batch | → `skipped_no_data`, no crash |
| Full stack restart | → data persists in volumes |

This is **production-style thinking** — not just happy-path demos.

---

## 19. Exactly-Once vs At-Least-Once Semantics

### At-least-once

Messages are delivered **one or more times**. Duplicates possible.

**Kafka default.** This project assumes at-least-once.

### Exactly-once

Messages processed **exactly one time**. No duplicates.

**Very hard** in distributed systems. This project achieves **effectively-once** through:
- Dedup by `event_id`
- Idempotent upserts
- Checkpoint-based offset tracking

### End-to-end exactly-once

Would require transactional Kafka + transactional database writes. Not implemented here (complexity vs benefit for a learning project).

---

## 20. String Prices (Decimal as String)

### What is it?

Prices and quantities are stored as **strings**, not floats.

### Why?

```python
>>> 0.1 + 0.2
0.30000000000000004
```

Floating-point arithmetic introduces rounding errors. In financial data, that's unacceptable.

### How this project handles it

- Producer: `str(data["p"])` — keep as string
- Validation: `Decimal(str(value))` — precise arithmetic
- Spark: `DecimalType(20, 8)` — fixed-precision column type
- PostgreSQL: `NUMERIC(20,8)` — exact decimal storage

---

## Concept map: how they connect

```text
ETL
 ├── Extract → Binance APIs
 ├── Transform → Validation, Dedup, Watermark, Window Aggregation
 └── Load → Parquet (data lake), PostgreSQL (serving)

Streaming (speed layer)
 ├── Event-driven (Kafka)
 ├── Micro-batching (5s trigger)
 ├── Checkpointing (fault tolerance)
 └── Watermarking (late events)

Batch (batch layer)
 ├── Orchestration (Airflow)
 ├── Compaction (Parquet optimization)
 └── Daily aggregation (VWAP)

Quality
 ├── Schema validation (contract)
 ├── DLQ (individual bad records)
 └── DQ gates (systemic bad data)

Resilience
 ├── Deduplication (at-least-once → effectively-once)
 ├── Idempotent upserts (safe retries)
 └── Fault injection (prove it works)
```

---

## Summary table

| Concept | One-line explanation |
|---------|---------------------|
| ETL | Extract from Binance, Transform with Spark, Load to Parquet/Postgres |
| Streaming vs Batch | Real-time candles vs scheduled backfill/compaction |
| Event-driven | Services talk through Kafka, not direct calls |
| Schema validation | Formal contract prevents bad data propagation |
| DLQ | Bad records quarantined, pipeline continues |
| Dedup | Same event_id counted only once |
| Watermarking | Handle late-arriving events with time bounds |
| Window aggregation | Group trades into 1-minute OHLC candles |
| Micro-batching | Process events in 5-second Spark batches |
| Checkpointing | Resume from last offset after crash |
| Idempotent writes | Retries don't create duplicates |
| Parquet | Compressed columnar raw storage |
| Data quality gates | Fail Airflow when DLQ ratio too high |
| Orchestration | Airflow schedules and retries batch jobs |
| Serving layer | PostgreSQL for dashboard queries |
| Observability | pipeline_metrics tracks every batch |
| String prices | Avoid floating-point money errors |

---

You now have the full documentation set. Return to the [index](./README.md) or ask me to go deeper on any concept.
