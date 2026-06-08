# Crypto Codebase ETL

End-to-end **hybrid batch + streaming** crypto analytics platform on live **Binance** market data.

Built for learning and resume use: real APIs, Kafka buffering, PySpark Structured Streaming, Parquet raw storage, PostgreSQL serving layer, Airflow orchestration, data quality gates, and a Streamlit dashboard — all runnable locally with Docker Compose.

---

## Stack

| Layer | Technology |
|-------|------------|
| Ingestion | Python, Binance WebSocket + REST |
| Messaging | Apache Kafka (`crypto_trades`, `dead_letter_queue`) |
| Streaming | PySpark Structured Streaming (DataFrame API only) |
| Batch | PySpark + Apache Airflow |
| Storage | Parquet (raw zone), PostgreSQL (serving) |
| Dashboard | Streamlit + Plotly |
| Runtime | Docker Compose |

**Tracked symbols (default):** `BTCUSDT`, `ETHUSDT`, `SOLUSDT`

---

## Architecture

```text
                         +------------------+
                         |  Binance WebSocket|
                         +--------+---------+
                                  |
                                  v
                         +--------+---------+
                         | Python Producer  |
                         | shared validation|
                         +--------+---------+
                                  |
                    valid         |         invalid
                      v           |           v
              +-------+---+       |    +------+------+
              | crypto_trades    |    | dead_letter |
              | (Kafka topic)    |    | _queue      |
              +-------+---+       |    +-------------+
                      |
                      v
              +-------+-----------+
              | Spark Structured  |
              | Streaming         |
              | validate/dedup/   |
              | watermark/1m OHLC |
              +---+---+-----+-----+
                  |   |     |
         raw      |   |     |  invalid
         Parquet  |   |     +----> DLQ (Kafka)
                  |   |
                  |   +--> PostgreSQL
                  |        - analytics.candles_1m
                  |        - analytics.pipeline_metrics
                  v
           data/raw/trades/

Airflow (scheduled)
  - hourly REST backfill  --> raw Parquet
  - daily compact/summary --> trades_compacted + analytics.daily_summary
  - daily data quality    --> fail on high DLQ ratio

Streamlit dashboard --> reads PostgreSQL (charts + pipeline health)
```

### Data-quality flow (fail safe, not fail loud)

```text
Event --> shared.validate_trade() --> pass --> main pipeline
                                   --> fail --> DLQ envelope --> dead_letter_queue
```

Validation rules live in [`shared/validation.py`](shared/validation.py) and are reused by:

- WebSocket producer
- Spark streaming (second pass)
- REST backfill batch job

---

## Project layout

```text
shared/                 # Schema, validation, DLQ envelope, DQ helpers
services/
  producer/             # Binance WS -> Kafka
  spark_streaming/      # Kafka -> Parquet + PostgreSQL
  spark_batch/          # compact, daily summary, REST backfill, DQ checks
infra/
  kafka/                # Topic bootstrap
  postgres/             # Analytics schema + Airflow DB init
  spark/conf/           # Spark defaults
airflow/dags/           # hourly backfill, daily batch, data quality
dashboard/              # Streamlit UI
tests/                  # pytest (no Docker required)
schemas/                # trade_event.json contract
data/                   # local Parquet + checkpoints (gitignored)
```

---

## Quick start

### 1) Prerequisites

- Docker Desktop (with Compose)
- Python 3.11+ (for local unit tests)
- ~8 GB RAM recommended (Kafka + Spark + Airflow)

### 2) Configure environment

```bash
cp .env.example .env
```

Key variables:

| Variable | Purpose |
|----------|---------|
| `SYMBOLS` | Comma-separated pairs to track |
| `KAFKA_TOPIC_TRADES` / `KAFKA_TOPIC_DLQ` | Main and dead-letter topics |
| `RAW_DATA_DIR` | Parquet raw zone mount (`/data/raw/trades` in containers) |
| `WATERMARK_MINUTES` | Spark lateness window (`10` or `10 minutes`) |
| `DLQ_WARN_RATIO` / `DLQ_FAIL_RATIO` | Data quality thresholds (1% / 5%) |

### 3) Run unit tests (no cluster)

```bash
pip install -e ".[dev]"
make test
```

Expected: all tests pass (`tests/test_validation.py`, aggregation, dedup, batch, data quality).

### 4) Start the full stack

```bash
make up
# or
docker compose --env-file .env up -d
```

### 5) Verify services

| Service | URL / port |
|---------|------------|
| Streamlit dashboard | http://localhost:8501 |
| Airflow UI | http://localhost:8081 (login: `admin` / `admin`) |
| PostgreSQL | localhost:5432 |
| Kafka (host) | localhost:9092 |

Check container health:

```bash
docker compose ps
docker compose logs -f producer spark-streaming
```

### 6) Confirm data is flowing

**Kafka (valid trades):**

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic crypto_trades \
  --from-beginning \
  --max-messages 3
```

**PostgreSQL (1m candles):**

```bash
docker compose exec postgres psql -U crypto -d crypto_analytics -c \
  "SELECT symbol, window_start, close_price, volume FROM analytics.candles_1m ORDER BY window_start DESC LIMIT 5;"
```

**Dashboard:** open http://localhost:8501 → Analytics + Pipeline Health tabs.

### 7) Trigger Airflow DAGs (manual first run)

**Without the UI** (scheduler only — works when webserver is OOM):

```powershell
.\scripts\airflow-trigger.ps1 crypto_hourly_backfill
.\scripts\airflow-trigger.ps1 crypto_daily_batch
.\scripts\airflow-trigger.ps1 crypto_data_quality
```

Or:

```powershell
docker compose exec airflow-scheduler airflow dags trigger crypto_hourly_backfill
docker compose exec airflow-scheduler airflow dags list-runs -d crypto_hourly_backfill
```

In Airflow UI (`http://localhost:8081`, login `admin` / `admin`), enable and trigger:

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `crypto_hourly_backfill` | `@hourly` | REST gap-fill with validation |
| `crypto_daily_batch` | `@daily` | Parquet compaction + daily summary |
| `crypto_data_quality` | `@daily` | DLQ ratio gate |

---

## Transformation style (important)

All Spark transformations use the **PySpark DataFrame API** (`select`, `filter`, `groupBy().agg()`, `withWatermark`, etc.).

No inline `spark.sql("...")` strings in transformation code.

Reference modules:

- [`services/spark_streaming/transforms.py`](services/spark_streaming/transforms.py)
- [`services/spark_batch/daily_summary.py`](services/spark_batch/daily_summary.py)

---

## PostgreSQL tables

| Table | Purpose |
|-------|---------|
| `analytics.candles_1m` | 1-minute OHLC, volume, volatility, avg price |
| `analytics.daily_summary` | Batch VWAP/high/low/volume per day |
| `analytics.pipeline_metrics` | Throughput, DLQ counts, batch duration, kafka lag |
| `analytics.dlq_events` | Optional DLQ audit table |

---

## Testing strategy

| Test file | What it proves |
|-----------|----------------|
| `tests/test_validation.py` | Shared validation + DLQ envelope |
| `tests/test_aggregation.py` | 1m OHLC/volume/avg correctness |
| `tests/test_dedup.py` | Duplicate `event_id` handling |
| `tests/test_batch_daily.py` | Daily VWAP math |
| `tests/test_backfill_rest.py` | REST normalize + reject bad rows |
| `tests/test_data_quality.py` | DLQ warn/fail thresholds |
| `tests/test_transforms.py` | Watermark interval normalization |

Run:

```bash
make test
make lint
```

---

## Fault-injection checklist (learning / demo)

Use these to prove the pipeline is production-style, not just happy-path.

| # | Inject fault | Expected behavior | How to verify |
|---|--------------|-------------------|---------------|
| 1 | Publish trade with empty price (manual Kafka message) | Rejected to `dead_letter_queue`, stream continues | DLQ consumer + `records_dlq` in metrics |
| 2 | Send duplicate `event_id` (format: `BTCUSDT-{trade_id}-{trade_time_ms}`) | Only one candle contribution after dedup | Volume not double-counted in `candles_1m` |
| 3 | Stop producer for 2 minutes | Spark/checkpoints resume without crash | `docker compose restart producer`, no PK violations |
| 4 | Stop `spark-streaming` for 1 minute | Kafka buffers, lag recovers | `pipeline_metrics.kafka_lag`, charts catch up |
| 5 | Set invalid symbol in payload | `INVALID_SYMBOL` in DLQ | DLQ envelope `error_code` |
| 6 | Run `crypto_data_quality` with high DLQ | DAG fails above `DLQ_FAIL_RATIO` | Airflow task state = failed |
| 7 | Run daily batch with no raw data | Job skips gracefully (`skipped_no_data`) | `pipeline_metrics.status` |
| 8 | Restart full stack | Topics/tables persist (volumes) | `docker compose down` (no `-v`) then `up` |

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| No Kafka messages | Producer not running / WS disconnect | `docker compose logs producer` |
| Spark not writing candles | Checkpoint issue or validation filtering all rows | Check `spark-streaming` logs + `pipeline_metrics` |
| Spark `OffsetOutOfRangeException` / crash loop | Stale checkpoints after Kafka restart | `.\scripts\reset-spark-checkpoints.ps1` (or delete `data/checkpoints/stream_trades`) |
| High `LATE_EVENT` DLQ during catch-up | Watermark used as DLQ threshold (fixed) | Set `LATE_EVENT_AFTER_MINUTES=30` (separate from `WATERMARK_MINUTES`) |
| Empty dashboard | Postgres up but no stream yet | Wait 1–2 min after producer + spark start |
| `relation "analytics.candles_1m" does not exist` | Postgres volume created before `init.sql` ran | `make db-init` (or apply `infra/postgres/init.sql` via psql) |
| Airflow `ERR_EMPTY_RESPONSE` on :8081 | Webserver OOM or still booting (common on 8 GB RAM) | Use slim webserver image (`Dockerfile.webserver`); wait 3–5 min; or trigger DAGs via CLI (see below) |
| Cannot find Docker memory slider | Docker Desktop on WSL2 uses `.wslconfig` | Create `%UserProfile%\.wslconfig` with `[wsl2]` `memory=10GB`, then `wsl --shutdown` and restart Docker |
| Airflow task fails on backfill | Network/API rate limit | Retry; check `BACKFILL_LOOKBACK_HOURS` |
| `WATERMARK_MINUTES=10` Spark error | Missing unit | Fixed via `shared/watermark.py` normalizer |
| Windows `./data` permission errors | Bind mount permissions | Use WSL2 path or pre-create `data/` folder |

**Reset Spark checkpoints only** (keeps Postgres/Kafka data):

```powershell
.\scripts\reset-spark-checkpoints.ps1
```

**Reset local state (destructive):**

```bash
docker compose down -v
rm -rf data/raw data/checkpoints
docker compose up -d
```

---

## Makefile commands

```bash
make install       # runtime deps
make dev-install   # runtime + pytest + ruff
make test          # unit tests
make lint          # ruff on shared/tests/services
make up            # docker compose up -d
make down          # docker compose down
```

---

## Resume positioning

**One-liner:**

> Built an end-to-end hybrid batch + streaming crypto analytics platform using Kafka, PySpark, PostgreSQL, Airflow, and Docker on live Binance data — with schema validation, dead-letter queues, deduplication, watermarking, unit-tested transforms, and operational dashboards.

**Bullet examples:**

- Designed a fail-safe ingestion pipeline: validated trade events at producer and Spark stages, routing bad records to a Kafka DLQ without stopping the stream.
- Implemented PySpark Structured Streaming micro-batches for 1-minute OHLC aggregations with watermarking, deduplication, Parquet raw storage, and idempotent PostgreSQL upserts.
- Orchestrated hourly REST backfill and daily compaction/summary jobs in Airflow with retry policies and DLQ-ratio data quality gates.
- Built a Streamlit analytics dashboard backed by PostgreSQL serving tables and pipeline health metrics (throughput, DLQ ratio, kafka lag).

---

## License

MIT
