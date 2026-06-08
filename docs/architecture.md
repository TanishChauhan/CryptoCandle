# Architecture

## Overview

This project uses a **hybrid lambda architecture** pattern (sometimes called **speed layer + batch layer**):

- **Speed layer** — Spark Structured Streaming processes live Kafka events in micro-batches
- **Batch layer** — Airflow triggers PySpark jobs on a schedule for backfill, compaction, and summaries
- **Serving layer** — PostgreSQL tables queried by the Streamlit dashboard

All services run in a single Docker Compose network (`crypto-net`) on your local machine.

---

## Architecture diagram

```text
                         +----------------------+
                         |   Binance Exchange   |
                         |  WebSocket + REST    |
                         +----------+-----------+
                                    |
                    live trades     |     historical aggTrades
                                    v
                         +----------+-----------+
                         |   Python Producer    |
                         |  (services/producer) |
                         |  validate + route    |
                         +----------+-----------+
                                    |
              valid                 |                 invalid
                v                   |                   v
        +-------+--------+          |          +--------+--------+
        | crypto_trades  |          |          | dead_letter_queue|
        | (Kafka topic)  |          |          | (Kafka topic)    |
        +-------+--------+          |          +------------------+
                |
                v
        +-------+---------------------------+
        |   Spark Structured Streaming      |
        |   (services/spark_streaming)      |
        |   parse → validate → dedup →      |
        |   watermark → 1m OHLC aggregate   |
        +---+---+---------------+-----------+
            |   |               |
   raw      |   |  candles      |  invalid (2nd pass)
   Parquet  |   |  upsert       |
            |   v               v
            |  +--------+   DLQ Kafka
            |  | Postgres|
            |  | analytics|
            |  +----+---+
            |       |
            v       v
     data/raw/   Streamlit
     trades/     Dashboard

        +-------+---------------------------+
        |   Apache Airflow (scheduler)      |
        |   + hourly REST backfill          |
        |   + daily compact + summary       |
        |   + daily DLQ quality gate        |
        +-------+---------------------------+
                |
                v
        Parquet compacted + analytics.daily_summary
```

---

## Architectural layers

### Layer 1: Ingestion

| Component | Technology | Responsibility |
|-----------|------------|----------------|
| Producer | Python + asyncio + websockets | Connect to Binance combined trade stream |
| Kafka client | confluent-kafka | Publish validated JSON to topics |

**Design choice:** Validation happens **at ingestion** so bad data never pollutes downstream Spark state.

### Layer 2: Messaging (buffer)

| Component | Technology | Responsibility |
|-----------|------------|----------------|
| Zookeeper | Confluent image | Kafka cluster coordination |
| Kafka | Confluent 7.6 | Durable event log between producer and Spark |
| kafka-init | Shell script | Create topics with retention policies |

**Design choice:** Kafka **decouples** the producer from Spark. If Spark restarts, events buffer in Kafka (up to 7 days retention on `crypto_trades`).

### Layer 3: Stream processing

| Component | Technology | Responsibility |
|-----------|------------|----------------|
| spark-streaming | PySpark Structured Streaming | Read Kafka, transform, write sinks |

The streaming job runs **three parallel write streams** from one Kafka source:

1. **Raw valid trades** → Parquet (`foreachBatch`)
2. **1m aggregated candles** → PostgreSQL (`foreachBatch` + upsert)
3. **Invalid records** → DLQ Kafka topic (`foreachBatch`)

Each stream has its **own checkpoint directory** for independent fault tolerance.

### Layer 4: Batch processing

| Component | Technology | Responsibility |
|-----------|------------|----------------|
| airflow-scheduler | Airflow 2.x + LocalExecutor | Run DAGs on schedule |
| spark_batch modules | PySpark (batch mode) | Backfill, compact, summarize, DQ |

Batch jobs share the same `shared/` validation logic and write to the same Parquet/Postgres paths.

### Layer 5: Storage

| Store | Format | Role |
|-------|--------|------|
| `data/raw/trades/` | Parquet (partitioned by year/month/day/hour) | Immutable raw zone |
| `data/raw/trades_compacted/` | Parquet (compacted daily) | Fewer files, faster batch reads |
| `data/checkpoints/` | Spark checkpoint files | Streaming recovery state |
| PostgreSQL `analytics.*` | Relational tables | Serving layer for queries |
| Kafka topics | JSON messages | Real-time buffer + DLQ audit trail |

### Layer 6: Serving & observability

| Component | Technology | Responsibility |
|-----------|------------|----------------|
| dashboard | Streamlit + Plotly | Charts and pipeline health UI |
| pipeline_metrics table | PostgreSQL | Per-batch throughput, DLQ counts, duration |

---

## Service topology (Docker Compose)

```text
crypto-net (bridge network)
├── zookeeper
├── kafka
├── kafka-init          (one-shot: create topics)
├── postgres            (analytics DB + Airflow metadata DB)
├── postgres-schema     (one-shot: apply init.sql)
├── producer            (long-running)
├── spark-streaming     (long-running)
├── airflow-init        (one-shot: migrate DB, create admin user)
├── airflow-webserver   (UI on :8081)
├── airflow-scheduler   (runs DAGs)
└── dashboard           (UI on :8501)
```

### Dependency chain (startup order)

```text
zookeeper → kafka → kafka-init → producer, spark-streaming
postgres → postgres-schema → dashboard
postgres → airflow-init → airflow-webserver, airflow-scheduler
```

`producer` and `spark-streaming` wait for `kafka-init` to finish so topics exist before they connect.

---

## Data zones (medallion-style, simplified)

This project uses a simplified **medallion** pattern:

| Zone | Location | Contents | Written by |
|------|----------|----------|------------|
| **Raw** | `data/raw/trades/` | Valid individual trades | Streaming + REST backfill |
| **Compacted** | `data/raw/trades_compacted/` | Deduplicated, fewer files | Daily batch |
| **Curated (stream)** | `analytics.candles_1m` | 1-minute OHLC | Spark streaming |
| **Curated (batch)** | `analytics.daily_summary` | Daily VWAP/high/low/volume | Daily batch |

There is no separate "bronze/silver/gold" naming, but the idea is the same: raw → cleaned → aggregated.

---

## Shared validation architecture

A critical architectural decision: **one validation contract, many enforcement points**.

```text
shared/validation.py  ←── used by ──→  Producer (Python)
                    ←── mirrored by ──→  Spark transforms (DataFrame expressions)
                    ←── used by ──→  REST backfill (Python)
```

The JSON schema in `schemas/trade_event.json` and the Pydantic model in `shared/schema.py` define the contract. This prevents "producer accepts it, Spark rejects it" drift.

---

## Fault tolerance patterns

| Failure | Architecture response |
|---------|----------------------|
| WebSocket disconnect | Producer exponential backoff reconnect |
| Bad trade event | Route to DLQ; stream continues |
| Spark streaming crash | Resume from checkpoint + Kafka offsets |
| Kafka temporarily ahead of Spark | Kafka buffers; lag recovers on restart |
| Missing raw data for a day | Daily batch logs `skipped_no_data`, does not crash |
| High DLQ ratio | Airflow DQ DAG fails (human intervention) |

---

## Three-stream Spark design (important detail)

`stream_job.py` does **not** use a single output. It forks the validated DataFrame:

```text
df_kafka
  → parse_kafka_trades
  → validate_and_enrich
       ├── df_valid  → raw Parquet sink (before dedup)
       │            → dedup + watermark → aggregate → Postgres candles
       └── df_invalid → DLQ Kafka sink
```

**Why raw sink is before dedup?**  
The raw zone stores *incoming valid trades* as they arrived. Deduplication is an aggregation concern; the archive should reflect what passed validation.

---

## Airflow's role in the architecture

Airflow is **orchestration only** — it does not transform data itself. Each DAG is a thin wrapper:

```text
Airflow DAG  →  PythonOperator  →  services/spark_batch/*.py
```

| DAG | Schedule | Calls |
|-----|----------|-------|
| `crypto_hourly_backfill` | `@hourly` | `run_hourly_backfill()` |
| `crypto_daily_batch` | `@daily` | `compact_day()` + `run_daily_summary()` |
| `crypto_data_quality` | `@daily` | `run_dlq_quality_check()` |

DAGs are **paused at creation** (`DAGS_ARE_PAUSED_AT_CREATION=true`). You must enable them in the UI or trigger manually.

---

## Network and ports (external access)

| Service | Host port | Purpose |
|---------|-----------|---------|
| Kafka | 9092 | External clients (optional) |
| PostgreSQL | 5432 | SQL clients, debugging |
| Airflow UI | 8081 | DAG management |
| Streamlit | 8501 | Dashboard |

Inside Docker, services use internal hostnames (`kafka:29092`, `postgres:5432`).

---

## What makes this "production-style" (even locally)

1. **Schema contract** with JSON schema + Pydantic
2. **DLQ** at producer and Spark stages
3. **Idempotent Postgres upserts** (`ON CONFLICT DO UPDATE`)
4. **Checkpoints** for exactly-once-ish streaming semantics
5. **Partitioned Parquet** for scalable reads
6. **Metrics table** for every micro-batch and batch job
7. **Unit tests** without Docker for core logic
8. **Configurable thresholds** via `.env`

---

## Summary

| Aspect | Design |
|--------|--------|
| Pattern | Hybrid streaming + batch (lambda-lite) |
| Messaging | Kafka as durable buffer |
| Compute | PySpark (stream + batch) |
| Serving | PostgreSQL + Streamlit |
| Orchestration | Airflow scheduled DAGs |
| Quality | Shared validation + DLQ + ratio gates |
| Runtime | Docker Compose single-network deployment |

Next: [Repo Structure](./repo-structure.md) — where each piece lives in the codebase.
