# Key Components

A detailed breakdown of **every major component** in the system — what it does, how it works internally, and how it connects to other parts.

---

## 1. Producer (`services/producer/`)

### Purpose

Ingest live trades from Binance and publish validated events to Kafka.

### Main loop (`main.py`)

```text
while True:
    connect WebSocket
    while connected:
        receive message
        normalize → validate → route (Kafka or DLQ)
    on disconnect:
        exponential backoff → reconnect
```

### Configuration (`ProducerConfig`)

Loaded from environment variables:

| Variable | Default | Effect |
|----------|---------|--------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker address |
| `KAFKA_TOPIC_TRADES` | `crypto_trades` | Valid events topic |
| `KAFKA_TOPIC_DLQ` | `dead_letter_queue` | Rejected events topic |
| `BINANCE_WS_BASE` | `wss://stream.binance.com:9443` | WebSocket endpoint |
| `SYMBOLS` | `BTCUSDT,ETHUSDT,SOLUSDT` | Pairs to subscribe |
| `LOG_LEVEL` | `INFO` | Logging verbosity |
| `LOG_JSON` | `true` | JSON structured logs |

### Binance WebSocket (`binance_ws.py`)

**`build_combined_trade_stream_url()`** — Creates a single connection for multiple symbols:

```text
wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/solusdt@trade
```

**`normalize_trade_payload()`** — Converts Binance format to TradeEvent. Computes `event_id` as `{symbol}-{trade_id}-{trade_time_ms}`.

**`connect()`** — WebSocket with keepalive (`ping_interval=20`, `max_queue=1000`).

### Kafka client (`kafka_client.py`)

Thin wrapper around `confluent_kafka.Producer`:

- `produce_json(topic, value, key)` — serializes dict to JSON bytes
- `flush()` — ensures delivery before reconnect

### Resilience

- WebSocket disconnect → exponential backoff (2^n seconds, max 60s)
- Bad messages → DLQ, loop continues
- Metrics logged every 60 seconds

---

## 2. Spark Streaming (`services/spark_streaming/`)

### Purpose

Consume Kafka trades, validate, aggregate into 1-minute candles, write to Parquet and PostgreSQL.

### Entry point (`stream_job.py`)

Creates **three independent streaming queries** from one Kafka source:

| Stream | Input | Output | Checkpoint |
|--------|-------|--------|------------|
| Raw | `df_valid` | Parquet | `checkpoints/.../raw_valid_trades` |
| Candles | `df_agg` | PostgreSQL | `checkpoints/.../valid` |
| Invalid | `df_invalid` | Kafka DLQ | `checkpoints/.../invalid` |

All triggered every `STREAM_TRIGGER_SECONDS` (default 5s).

### Transforms (`transforms.py`)

#### `parse_kafka_trades(df_kafka)`

Kafka binary `value` → JSON string → typed DataFrame columns.

#### `validate_and_enrich(df_trades, ...)`

Adds validation columns using **only DataFrame expressions** (no SQL strings). Mirrors `shared/validation.py` rules plus `LATE_EVENT` detection.

#### `dedup_and_watermark(valid_df, watermark_minutes)`

```python
valid_df
  .withWatermark("trade_time", watermark_minutes)
  .dropDuplicates(["event_id"])
```

Watermark lateness comes from `WATERMARK_MINUTES` env (normalized by `shared/watermark.py`).

#### `aggregate_1m_ohlc(valid_dedup_df)`

Groups by `(symbol, 1-minute window)` and computes OHLC, volume, volatility.

Uses `min_by`/`max_by` for deterministic open/close by timestamp.

### Sinks (`sinks.py`)

**`write_raw_trades_parquet()`** — Appends with `year/month/day/hour` partitions.

**`enrich_invalid_for_dlq()`** — Builds DLQ envelope struct, serializes to JSON for Kafka sink.

### PostgreSQL writer (`pg_writer.py`)

**`upsert_candles_1m(rows)`** — Batch insert with `ON CONFLICT DO UPDATE`. Uses `psycopg2.extras.execute_values` for efficiency.

**`insert_pipeline_metrics(...)`** — Logs every micro-batch result to `analytics.pipeline_metrics`.

---

## 3. Spark Batch (`services/spark_batch/`)

### Purpose

Scheduled heavy processing: backfill, compaction, daily summaries, data quality.

### Common utilities (`common.py`)

| Function | Purpose |
|----------|---------|
| `build_spark(app_name)` | Create SparkSession with UTC timezone |
| `resolve_trade_date(date_str)` | Default to yesterday if not specified |
| `load_symbols()` | Parse `SYMBOLS` env var |
| `partition_path(base_dir, date)` | Build `year=.../month=.../day=...` path |

### REST backfill (`backfill_rest.py`)

**`fetch_agg_trades()`** — HTTP GET to Binance with `startTime`, `endTime`, `limit=1000`.

**`run_hourly_backfill()`** — For each symbol:
1. Fetch trades for last N hours (`BACKFILL_LOOKBACK_HOURS`, default 2)
2. Normalize and validate
3. Write valid rows to raw Parquet via Spark
4. Log metrics

### Compaction (`compact_parquet.py`)

**`compact_day(spark, trade_date, coalesce_partitions)`**:
1. Read all Parquet for the date
2. `dropDuplicates(["event_id"])`
3. `coalesce(N)` → fewer files
4. Write to `trades_compacted` partitioned by hour

### Daily summary (`daily_summary.py`)

**`build_daily_summary_df(df)`** — Computes VWAP:

```text
VWAP = sum(price × quantity) / sum(quantity)
```

**`run_daily_summary()`** — Reads compacted (or raw) data, computes summaries, upserts to `analytics.daily_summary`.

### Data quality (`data_quality.py`)

**`fetch_recent_pipeline_totals()`** — SQL aggregate over `pipeline_metrics`.

**`run_dlq_quality_check()`** — Computes ratio, evaluates against thresholds from `shared/dq_quality.py`.

### CLI entry (`batch_job.py`)

```bash
python -m services.spark_batch.batch_job compact --date 2024-04-05
python -m services.spark_batch.batch_job daily
python -m services.spark_batch.batch_job backfill
python -m services.spark_batch.batch_job all
```

---

## 4. Shared library (`shared/`)

### `schema.py` — TradeEvent

Pydantic model with `extra="forbid"` — rejects unknown fields.

### `validation.py` — validate_trade()

Central validation logic:

| Check | Error code |
|-------|------------|
| Not a dict / schema fail | `MALFORMED_JSON` / `SCHEMA_VALIDATION_FAILED` |
| Empty price | `NULL_PRICE` |
| Non-numeric or ≤0 price | `INVALID_PRICE` |
| Empty quantity | `NULL_QUANTITY` |
| Negative quantity | `NEGATIVE_QUANTITY` |
| Symbol not in allowed set | `INVALID_SYMBOL` |
| Timestamp out of bounds | `INVALID_TIMESTAMP` |

Default time skew: ±24 hours. Backfill uses ±7 days (`BACKFILL_MAX_TIME_SKEW_MS`).

### `dlq.py` — build_dlq_envelope()

Standard DLQ record format used by producer and Spark.

### `watermark.py` — normalize_watermark_interval()

Handles env values like `10`, `10 minutes`, `2 minutes` → Spark-compatible interval string.

### `aggregation_ref.py` — Reference implementations

Pure Python versions of Spark aggregation logic. Used by unit tests to prove Spark output matches expected math.

### `dq_quality.py` — Threshold evaluation

| Ratio | Status |
|-------|--------|
| < 1% | `ok` |
| 1% – 5% | `warn` |
| ≥ 5% | `fail` |

Thresholds configurable via `DLQ_WARN_RATIO` and `DLQ_FAIL_RATIO`.

---

## 5. Airflow DAGs (`airflow/dags/`)

### `crypto_hourly_backfill`

| Property | Value |
|----------|-------|
| Schedule | `@hourly` |
| Retries | 2 (5 min delay) |
| Task | `run_hourly_backfill()` |
| catchup | `false` |

### `crypto_daily_batch`

| Property | Value |
|----------|-------|
| Schedule | `@daily` |
| Task | `compact_day()` then `run_daily_summary()` |
| Date | Yesterday (via `resolve_trade_date`) |

### `crypto_data_quality`

| Property | Value |
|----------|-------|
| Schedule | `@daily` |
| Task | `run_dlq_quality_check(lookback_hours=24)` |
| On fail | Raises `RuntimeError` → DAG task fails |

All DAGs: `max_active_runs=1`, `DAGS_ARE_PAUSED_AT_CREATION=true`.

---

## 6. Dashboard (`dashboard/`)

### `app.py` — Streamlit application

**Analytics tab:**
- Symbol selector (BTCUSDT, ETHUSDT, SOLUSDT)
- Plotly candlestick chart (OHLC)
- Average price overlay
- Volume bar chart
- Volatility line chart
- Daily summary table
- Recent candles data table

**Pipeline Health tab:**
- Records in/out over time
- DLQ count bars
- Batch duration trend
- DLQ ratio gauge with warn/fail thresholds
- Recent DLQ events table

### `db.py` — Database queries

| Function | Query target |
|----------|-------------|
| `fetch_candles(symbol)` | `analytics.candles_1m` (last 200 rows) |
| `fetch_daily_summary(symbol)` | `analytics.daily_summary` |
| `fetch_pipeline_metrics()` | `analytics.pipeline_metrics` (last 24h) |
| `fetch_dlq_summary()` | `analytics.dlq_events` |
| `get_symbols()` | From `SYMBOLS` env or DISTINCT query |

Uses `@st.cache_data` for query caching between reruns.

---

## 7. Infrastructure (`infra/`)

### Kafka topic bootstrap (`kafka/init-topics.sh`)

| Topic | Partitions | Retention |
|-------|------------|-----------|
| `crypto_trades` | 3 | 7 days |
| `dead_letter_queue` | 1 | 30 days |

`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` — topics must be explicitly created.

### PostgreSQL schema (`postgres/init.sql`)

Creates `analytics` schema with four tables:

| Table | Primary key | Purpose |
|-------|-------------|---------|
| `candles_1m` | (symbol, window_start) | Stream candles |
| `daily_summary` | (trade_date, symbol) | Batch daily metrics |
| `pipeline_metrics` | (recorded_at, job_name) | Observability |
| `dlq_events` | (received_at) | DLQ audit |

Indexes on `(symbol, window_start DESC)`, `(job_name, recorded_at DESC)`, `(received_at DESC)`.

### Airflow DB init (`postgres/init-airflow-db.sh`)

Creates a separate `airflow` database for Airflow metadata (DAG runs, task instances, etc.).

---

## 8. Tests (`tests/`)

Each test file maps to a production concern:

| Test | Component tested | Why it matters |
|------|------------------|----------------|
| `test_validation.py` | `shared/validation.py` | Core data gate |
| `test_aggregation.py` | `shared/aggregation_ref.py` vs Spark | Correct OHLC math |
| `test_dedup.py` | Dedup logic | No double-counting |
| `test_transforms.py` | `shared/watermark.py` | Spark compatibility |
| `test_batch_daily.py` | VWAP calculation | Financial accuracy |
| `test_backfill_rest.py` | REST normalization | Gap-fill correctness |
| `test_data_quality.py` | `shared/dq_quality.py` | Threshold behavior |

Run without Docker: `make test`

---

## Component interaction matrix

| Component | Reads from | Writes to |
|-----------|-----------|-----------|
| Producer | Binance WS | Kafka (trades + DLQ) |
| Spark Streaming | Kafka trades | Parquet, Postgres, Kafka DLQ |
| Spark Batch (backfill) | Binance REST | Parquet |
| Spark Batch (compact) | Raw Parquet | Compacted Parquet |
| Spark Batch (daily) | Compacted/raw Parquet | Postgres daily_summary |
| Spark Batch (DQ) | Postgres pipeline_metrics | (pass/fail only) |
| Airflow | — | Triggers batch jobs |
| Dashboard | Postgres | — (read only) |

---

## Summary

| Component | One sentence |
|-----------|-------------|
| Producer | Binance → validated Kafka events |
| Spark Streaming | Kafka → candles + raw archive |
| Spark Batch | Parquet maintenance + daily rollups |
| Shared | Rules everyone agrees on |
| Airflow | Scheduling and retries |
| Dashboard | Visual proof it works |
| Infra | Topics, tables, configs |
| Tests | Math and logic correctness |

Next: [Deployment](./deployment.md) — how to run everything locally.
