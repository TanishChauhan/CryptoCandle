# Tech Stack

A complete guide to **every technology** in this project — what it does, where it's used, and why it was chosen. Written for beginners.

---

## Stack overview

| Layer | Technology | Version (approx.) |
|-------|------------|-------------------|
| Language | Python | 3.11+ |
| Ingestion | websockets, httpx, confluent-kafka | — |
| Messaging | Apache Kafka + Zookeeper | Confluent 7.6 |
| Stream compute | PySpark Structured Streaming | 3.5+ |
| Batch compute | PySpark + Apache Airflow | Airflow 2.x |
| Storage | Parquet, PostgreSQL 16 | — |
| Dashboard | Streamlit + Plotly + pandas | — |
| Validation | Pydantic v2 | 2.6+ |
| Logging | structlog | 24+ |
| Testing | pytest + ruff | — |
| Runtime | Docker Compose | — |

---

## Python 3.11+

**Role:** The primary language for everything except infrastructure config.

**Where used:**
- Producer service
- Shared library
- Dashboard
- Airflow DAG definitions (PythonOperator)
- Unit tests

**Why Python:**
- Excellent ecosystem for data engineering (PySpark, pandas, Airflow)
- Readable for learners
- Single language across ingestion, orchestration, and UI

**Project packaging:** `pyproject.toml` defines the `crypto-codebase-etl` package with `shared` as an installable module.

---

## Pydantic

**Role:** Data validation and schema enforcement.

**Where used:** `shared/schema.py` — the `TradeEvent` model.

```python
class TradeEvent(BaseModel):
    event_id: str
    symbol: str
    trade_id: int
    price: str
    ...
```

**Why Pydantic:**
- Validates types at runtime without manual `if` checks
- `extra="forbid"` rejects unknown fields (strict contract)
- Integrates with `validate_trade()` for structured error codes

**Beginner tip:** Pydantic is like a bouncer at the door — it checks every field before data enters the pipeline.

---

## structlog

**Role:** Structured logging (JSON or human-readable).

**Where used:** `services/producer/logging_config.py`, producer main loop.

**Why structlog:**
- Logs are machine-parseable (`LOG_JSON=true`)
- Key-value fields (`events_published=42`) instead of unstructured strings
- Industry standard for microservices observability

---

## Binance APIs

### WebSocket (`wss://stream.binance.com:9443`)

**Role:** Real-time trade stream.

**Where used:** `services/producer/binance_ws.py`

**How it works:**
- Combined stream URL: `/stream?streams=btcusdt@trade/ethusdt@trade/...`
- Each message wraps trade data in a `data` field
- Producer maintains persistent connection with ping/pong

### REST (`https://api.binance.com`)

**Role:** Historical trade backfill.

**Where used:** `services/spark_batch/backfill_rest.py`

**Endpoint:** `GET /api/v3/aggTrades` with `startTime`, `endTime`, `limit`

**Why both WS + REST:**
- WebSocket = low latency, but can miss data on disconnect
- REST = reliable gap-fill, but rate-limited and polled

---

## Apache Kafka

**Role:** Distributed event log / message broker.

**Where used:**
- Producer writes to `crypto_trades` and `dead_letter_queue`
- Spark streaming reads from `crypto_trades`
- Spark invalid stream writes to `dead_letter_queue`

### Key concepts

| Concept | In this project |
|---------|-----------------|
| **Topic** | Named channel (`crypto_trades`, `dead_letter_queue`) |
| **Partition** | `crypto_trades` has 3 partitions (parallelism potential) |
| **Offset** | Spark tracks how far it has read (via checkpoint) |
| **Retention** | 7 days for trades, 30 days for DLQ |
| **Key** | Symbol (e.g. `BTCUSDT`) — routes to consistent partition |

### Why Kafka (not direct Producer → Spark)?

1. **Buffering** — Spark can go down; Kafka holds events
2. **Decoupling** — Producer doesn't know about Spark
3. **Replay** — Re-read from earlier offsets for debugging
4. **Multiple consumers** — Could add another consumer later without changing producer

### Zookeeper

Kafka uses Zookeeper for cluster coordination. In production, Kafka 3.x+ can use KRaft (no Zookeeper), but Confluent 7.6 images still use the classic setup.

---

## PySpark (Apache Spark)

**Role:** Distributed data processing engine.

### Structured Streaming (real-time)

**Where used:** `services/spark_streaming/`

**How it works:**
- Reads Kafka as a streaming source
- Processes in **micro-batches** (every 5 seconds)
- Uses **checkpointing** for fault tolerance
- Outputs via `foreachBatch` for custom sinks (Parquet, Postgres, Kafka)

**Important project rule:** DataFrame API only — no `spark.sql("SELECT ...")` in transform code.

### Batch mode (scheduled)

**Where used:** `services/spark_batch/`

**Jobs:**
- Read/write Parquet
- `groupBy().agg()` for daily summaries
- `coalesce()` for file compaction

### Why Spark (not plain Python/pandas)?

- Same API for streaming and batch
- Handles partitioned Parquet at scale
- Window aggregations with watermarking built-in
- Industry standard for big data pipelines

**Beginner note:** For this project's data volume, pandas *could* work — but Spark demonstrates skills employers expect.

---

## Parquet

**Role:** Columnar file format for raw trade storage.

**Where used:**
- `data/raw/trades/` — append-only raw zone
- `data/raw/trades_compacted/` — daily compacted files

**Partitioning:** `year/month/day/hour` directories.

**Why Parquet:**
- Compressed (smaller than JSON/CSV)
- Columnar (fast aggregations on price/volume)
- Schema embedded in files
- Spark native format

---

## PostgreSQL 16

**Role:** Serving layer — tables that apps query.

**Where used:**
- `analytics.candles_1m` — stream output
- `analytics.daily_summary` — batch output
- `analytics.pipeline_metrics` — observability
- `analytics.dlq_events` — optional DLQ audit
- `airflow` database — Airflow metadata (separate DB)

**Why Postgres (not MongoDB/Elasticsearch)?**
- SQL is universal for dashboards and BI tools
- Strong typing (`NUMERIC` for prices)
- `ON CONFLICT` upserts for idempotent writes
- Single database for analytics + Airflow metadata

**Driver:** `psycopg2` for Python connections.

---

## Apache Airflow

**Role:** Workflow scheduler and orchestrator.

**Where used:** `airflow/dags/` — three DAGs.

### Components in this project

| Component | Image | Role |
|-----------|-------|------|
| airflow-init | Dockerfile.init | DB migration, create admin |
| airflow-scheduler | Dockerfile | Executes DAG tasks |
| airflow-webserver | Dockerfile.webserver | Web UI (slim, less RAM) |

**Executor:** `LocalExecutor` — tasks run as subprocesses on the same machine (fine for local dev).

### Why Airflow (not cron)?

- Dependency management between tasks
- Retry policies (`retries: 2`, `retry_delay: 5min`)
- Web UI for monitoring
- Scheduling with `catchup=False` (don't backfill missed runs)

---

## Streamlit

**Role:** Python-native web dashboard framework.

**Where used:** `dashboard/app.py`

**Features used:**
- `st.tabs()` — Analytics vs Pipeline Health
- `st.plotly_chart()` — interactive candlestick charts
- `st.cache_data` — cached DB queries (in `db.py`)
- Auto-refresh via page reruns

**Why Streamlit (not React/Grafana)?**
- Pure Python — no separate frontend codebase
- Fast to build for data science demos
- Good enough for local analytics dashboards

---

## Plotly

**Role:** Interactive charting library.

**Where used:** Candlestick charts, volume bars, volatility lines in `dashboard/app.py`.

**Chart types:**
- `go.Candlestick` — OHLC visualization
- `go.Bar` — volume
- `go.Scatter` — avg price, volatility

---

## pandas

**Role:** In-memory tabular data for the dashboard.

**Where used:** `dashboard/db.py` — SQL results → DataFrame → charts.

Not used in the core pipeline (Spark handles that).

---

## Docker & Docker Compose

**Role:** Containerize and orchestrate all services locally.

**Where used:** `docker-compose.yml` + per-service `Dockerfile`s.

### Why Docker?

- Reproducible environment (same on Windows/Mac/Linux)
- Isolated service dependencies
- One command to start 11 services
- Matches how production deployments work (Kubernetes, ECS, etc.)

### Volumes

| Volume | Purpose |
|--------|---------|
| `postgres_data` | Persist database across restarts |
| `./data:/data` | Parquet + checkpoints on host |

---

## pytest + ruff

### pytest

**Role:** Unit testing framework.

**Config:** `pyproject.toml` → `testpaths = ["tests"]`

**Key point:** Tests run **without Docker** — they test pure Python logic (validation, aggregation math, DQ thresholds).

### ruff

**Role:** Fast Python linter.

**Run:** `make lint` → checks `shared`, `tests`, `services`, `dashboard`.

---

## httpx

**Role:** HTTP client for REST backfill.

**Where used:** `backfill_rest.py` — calls Binance `aggTrades` API.

**Why httpx (not requests):** Modern async-capable client with clean API. Used synchronously here.

---

## confluent-kafka

**Role:** Python client for Apache Kafka.

**Where used:** `services/producer/kafka_client.py`

**Why confluent-kafka (not kafka-python):** Official Confluent client, better performance, matches the Confluent Docker images.

---

## websockets

**Role:** Async WebSocket client.

**Where used:** `services/producer/binance_ws.py`

**Config:** `ping_interval=20`, `max_queue=1000` for connection health.

---

## Dependency groups (pyproject.toml)

| Group | Packages | When to install |
|-------|----------|-----------------|
| Core | pydantic, structlog, confluent-kafka, websockets, httpx | Always (`pip install -e .`) |
| dev | pytest, pytest-cov, ruff | Development (`pip install -e ".[dev]"`) |
| spark | pyspark | Spark jobs (in Docker images) |
| dashboard | streamlit, plotly, pandas, psycopg2 | Dashboard container |

---

## What each technology teaches you

| Technology | Skill you gain |
|------------|----------------|
| Kafka | Event-driven architecture, pub/sub |
| PySpark | Distributed transforms, streaming windows |
| Airflow | Pipeline orchestration, scheduling |
| PostgreSQL | Dimensional serving tables, upserts |
| Parquet | Data lake storage patterns |
| Pydantic | Schema contracts, data validation |
| Docker | Containerized microservices |
| Streamlit | Rapid data app development |

---

## Summary

This stack is a **standard modern data engineering portfolio**:

```text
Source API → Kafka → Spark → Parquet + SQL → Dashboard
                    ↑
                 Airflow (batch)
```

Nothing exotic — every tool is commonly listed in data engineer job postings. The value is in how they are **wired together** with production patterns (DLQ, dedup, watermark, DQ gates).

Next: [Data Flow](./data-flow.md) — detailed view of how data moves and transforms.
