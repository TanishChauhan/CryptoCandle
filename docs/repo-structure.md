# Repo Structure

This document explains **every major folder and file group** in the repository. Use it as a map when reading code for the first time.

---

## Top-level tree

```text
crypto-codebase-ETL/
├── shared/                 # Cross-cutting Python library (validation, schema, DLQ)
├── services/               # Runnable applications (producer, spark jobs)
│   ├── producer/
│   ├── spark_streaming/
│   └── spark_batch/
├── airflow/                # Airflow Dockerfiles + DAG definitions
│   └── dags/
├── dashboard/              # Streamlit UI
├── infra/                  # Infrastructure bootstrap (Kafka, Postgres, Spark config)
├── tests/                  # Unit tests (pytest, no Docker)
├── schemas/                # JSON schema contracts
├── scripts/                # Helper scripts (Airflow trigger)
├── data/                   # Local Parquet + checkpoints (gitignored)
├── docs/                   # This documentation folder
├── docker-compose.yml      # Full stack definition
├── pyproject.toml          # Python package + dependencies
├── Makefile                # Common commands
├── .env.example            # Environment variable template
└── README.md               # Quick start guide
```

---

## `shared/` — The glue library

Everything that **multiple services must agree on** lives here. This is the most important folder for understanding consistency across the pipeline.

| File | Purpose |
|------|---------|
| `schema.py` | Pydantic `TradeEvent` model — the canonical data shape |
| `validation.py` | `validate_trade()` — price, quantity, symbol, timestamp rules |
| `dlq.py` | `build_dlq_envelope()` — standard format for rejected records |
| `watermark.py` | Normalizes `WATERMARK_MINUTES` env (e.g. `10` → `10 minutes`) |
| `aggregation_ref.py` | Pure-Python OHLC math for unit tests (mirrors Spark logic) |
| `dq_quality.py` | DLQ ratio computation and warn/fail threshold evaluation |

**Why it exists:** The producer validates in Python. Spark validates with DataFrame expressions. Backfill validates in Python again. All must apply the **same rules**.

The package is installable via `pip install -e .` (defined in `pyproject.toml`).

---

## `services/producer/` — Live ingestion

Connects to Binance WebSocket and publishes to Kafka.

| File | Purpose |
|------|---------|
| `main.py` | Main loop: connect WS → normalize → validate → Kafka/DLQ |
| `binance_ws.py` | URL builder, payload normalization, WebSocket helpers |
| `kafka_client.py` | Thin wrapper around confluent-kafka producer |
| `logging_config.py` | structlog setup (JSON or plain) |
| `Dockerfile` | Container image for the producer service |

**Entry point:** `main.py` → `run_producer()` → infinite reconnect loop.

**Key behavior:**
- Builds combined stream URL: `btcusdt@trade/ethusdt@trade/...`
- Creates `event_id` as `{symbol}-{trade_id}-{trade_time_ms}`
- Rejects bad events to DLQ without stopping

---

## `services/spark_streaming/` — Real-time processing

PySpark Structured Streaming job: Kafka → transforms → Parquet + Postgres + DLQ.

| File | Purpose |
|------|---------|
| `stream_job.py` | Main job: reads Kafka, wires three write streams |
| `transforms.py` | `parse_kafka_trades`, `validate_and_enrich`, `dedup_and_watermark`, `aggregate_1m_ohlc` |
| `sinks.py` | Parquet writer + DLQ envelope builder for Kafka sink |
| `pg_writer.py` | `upsert_candles_1m()`, `insert_pipeline_metrics()` |
| `Dockerfile` | Spark streaming container |

**Design rule (from README):** All transforms use the **DataFrame API only** — no inline `spark.sql("...")` strings.

---

## `services/spark_batch/` — Scheduled batch jobs

PySpark jobs invoked by Airflow or the CLI entrypoint.

| File | Purpose |
|------|---------|
| `batch_job.py` | CLI: `compact`, `daily`, `backfill`, `all` |
| `backfill_rest.py` | Hourly REST gap-fill from Binance `aggTrades` API |
| `backfill_utils.py` | Normalize REST payloads + validate rows |
| `compact_parquet.py` | Merge many small Parquet files into fewer files |
| `daily_summary.py` | Compute VWAP, high, low, volume per symbol per day |
| `data_quality.py` | Query `pipeline_metrics`, evaluate DLQ ratio |
| `common.py` | `build_spark()`, `resolve_trade_date()`, `load_symbols()` |
| `feature_prep.py` | Feature engineering helpers (extension point) |
| `pg_writer.py` | Batch-specific Postgres writers (daily summary upsert) |
| `Dockerfile` | Batch Spark image (used by Airflow scheduler) |

---

## `airflow/` — Orchestration

| Path | Purpose |
|------|---------|
| `dags/crypto_hourly_backfill.py` | `@hourly` DAG → REST backfill |
| `dags/crypto_daily_batch.py` | `@daily` DAG → compact + daily summary |
| `dags/crypto_data_quality.py` | `@daily` DAG → DLQ ratio gate (fails if too high) |
| `Dockerfile` | Full Airflow image for scheduler |
| `Dockerfile.webserver` | Slimmer image for UI (less memory) |
| `Dockerfile.init` | One-shot DB migration + admin user creation |

DAGs are minimal — they import and call functions from `services/spark_batch/`.

---

## `dashboard/` — Streamlit UI

| File | Purpose |
|------|---------|
| `app.py` | Two tabs: Analytics (candlestick charts) + Pipeline Health |
| `db.py` | PostgreSQL queries → pandas DataFrames |
| `Dockerfile` | Streamlit container on port 8501 |

Reads from `analytics.candles_1m`, `analytics.daily_summary`, `analytics.pipeline_metrics`, `analytics.dlq_events`.

---

## `infra/` — Bootstrap configuration

| Path | Purpose |
|------|---------|
| `kafka/init-topics.sh` | Creates `crypto_trades` (3 partitions, 7d retention) and `dead_letter_queue` (1 partition, 30d) |
| `postgres/init.sql` | Creates `analytics` schema and all tables |
| `postgres/init-airflow-db.sh` | Creates separate `airflow` database |
| `spark/conf/spark-defaults.conf` | Spark cluster defaults |

---

## `tests/` — Unit tests

Run with `make test` — **no Docker required**.

| File | What it proves |
|------|----------------|
| `test_validation.py` | Shared validation rules + DLQ envelope |
| `test_aggregation.py` | 1m OHLC math matches `aggregation_ref` |
| `test_dedup.py` | Duplicate `event_id` handling |
| `test_transforms.py` | Watermark interval normalization |
| `test_batch_daily.py` | Daily VWAP calculation |
| `test_backfill_rest.py` | REST normalize + reject bad rows |
| `test_data_quality.py` | DLQ warn/fail thresholds |
| `fixtures/` | Sample JSON trade data for tests |
| `conftest.py` | Shared pytest fixtures |

---

## `schemas/` — Data contracts

| File | Purpose |
|------|---------|
| `trade_event.json` | JSON Schema (draft-07) for the trade event shape |

Documents required fields: `event_id`, `symbol`, `trade_id`, `price`, `quantity`, `trade_time_ms`.

---

## `scripts/` — Operational helpers

| File | Purpose |
|------|---------|
| `airflow-trigger.ps1` | PowerShell script to trigger DAGs without the Airflow UI |

Useful when the Airflow webserver is OOM on low-RAM machines.

---

## `data/` — Runtime storage (gitignored)

Created at runtime when the stack runs. Not committed to git.

```text
data/
├── raw/
│   ├── trades/              # Streaming + backfill Parquet
│   └── trades_compacted/    # Daily compacted Parquet
└── checkpoints/
    └── stream_trades/       # Spark streaming checkpoints
        ├── raw_valid_trades/
        ├── valid/
        └── invalid/
```

Mounted into containers via `HOST_DATA_DIR=./data` → `/data`.

---

## Root configuration files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Defines all 11 services, networks, volumes |
| `pyproject.toml` | Package metadata, dependencies, pytest/ruff config |
| `Makefile` | `install`, `test`, `lint`, `up`, `down`, `db-init` |
| `.env.example` | All configurable environment variables |
| `.gitignore` | Excludes `data/`, `.env`, `__pycache__`, etc. |

---

## How code flows between folders

```text
Binance API
    ↓
services/producer/main.py
    uses → shared/validation.py, shared/dlq.py, shared/schema.py
    ↓
Kafka
    ↓
services/spark_streaming/stream_job.py
    uses → services/spark_streaming/transforms.py
    uses → shared/watermark.py (indirectly via env)
    ↓
Parquet (data/) + PostgreSQL (infra/postgres/init.sql tables)
    ↓
airflow/dags/*.py
    calls → services/spark_batch/*.py
    uses → shared/dq_quality.py
    ↓
dashboard/app.py
    reads → PostgreSQL via dashboard/db.py
```

---

## Docker build context

All service Dockerfiles use the **repository root** as build context (`context: .`). This lets them copy `shared/`, `services/`, and `pyproject.toml` into the image.

---

## Where to start reading code (beginner path)

| Order | File | Why |
|-------|------|-----|
| 1 | `shared/schema.py` | Understand the data shape |
| 2 | `shared/validation.py` | Understand what "valid" means |
| 3 | `services/producer/binance_ws.py` | See how Binance JSON becomes a TradeEvent |
| 4 | `services/producer/main.py` | See the full ingestion loop |
| 5 | `services/spark_streaming/transforms.py` | See streaming transforms |
| 6 | `services/spark_streaming/stream_job.py` | See how sinks are wired |
| 7 | `airflow/dags/crypto_daily_batch.py` | See how batch is triggered |
| 8 | `dashboard/app.py` | See how results are displayed |

---

## Summary

| Folder | One-line description |
|--------|---------------------|
| `shared/` | Shared rules and models |
| `services/producer/` | Binance → Kafka |
| `services/spark_streaming/` | Kafka → Parquet + Postgres |
| `services/spark_batch/` | Scheduled heavy processing |
| `airflow/` | When batch jobs run |
| `dashboard/` | Human-facing UI |
| `infra/` | DB schema, Kafka topics, Spark config |
| `tests/` | Proof that core logic is correct |
| `schemas/` | Formal data contract |
| `data/` | Local runtime storage |

Next: [End-to-End Workflow](./end-to-end-workflow.md) — follow one trade through the entire system.
