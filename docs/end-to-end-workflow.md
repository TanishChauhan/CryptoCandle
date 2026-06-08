# End-to-End Workflow

This document walks through **what happens from start to finish** — from you running `make up` to seeing a candlestick chart on the dashboard. We'll also trace **one single trade** through every stage.

---

## Part 1: Starting the system

### Step 1 — Configure environment

```bash
cp .env.example .env
```

You set variables like `SYMBOLS`, `WATERMARK_MINUTES`, and `DLQ_FAIL_RATIO`. These control behavior across all services.

### Step 2 — Start Docker Compose

```bash
make up
# equivalent to: docker compose --env-file .env up -d
```

Docker starts services in dependency order:

```text
1. zookeeper + postgres start
2. kafka starts (waits for zookeeper)
3. kafka-init runs once → creates crypto_trades + dead_letter_queue topics
4. postgres-schema runs once → applies analytics tables
5. airflow-init runs once → migrates Airflow DB, creates admin user
6. producer starts (waits for kafka-init)
7. spark-streaming starts (waits for kafka-init + postgres healthy)
8. airflow-webserver + airflow-scheduler start
9. dashboard starts (waits for postgres-schema)
```

### Step 3 — Data begins flowing (1–2 minutes)

Within about 1–2 minutes you should see:

- Kafka messages on `crypto_trades`
- Rows appearing in `analytics.candles_1m`
- Parquet files under `data/raw/trades/`
- Charts on http://localhost:8501

---

## Part 2: The live streaming workflow (continuous)

This runs 24/7 while the stack is up.

### Phase A — Ingestion (Producer)

```text
Every ~milliseconds:
  Binance WebSocket sends trade JSON
       ↓
  Producer receives message
       ↓
  normalize_trade_payload() creates TradeEvent fields
       ↓
  validate_trade() checks price, quantity, symbol, timestamp
       ↓
  ┌─ VALID ──→ publish to crypto_trades (key = symbol)
  └─ INVALID ─→ publish to dead_letter_queue (DLQ envelope)
```

The producer logs metrics every 60 seconds: `events_received`, `events_published`, `events_rejected`, `ws_disconnects`.

If the WebSocket drops, the producer waits with **exponential backoff** (2s, 4s, 8s … up to 60s) and reconnects.

### Phase B — Stream processing (Spark)

Every **5 seconds** (configurable via `STREAM_TRIGGER_SECONDS`), Spark processes a micro-batch:

```text
Read new Kafka messages from crypto_trades
       ↓
parse_kafka_trades() — JSON → typed columns
       ↓
validate_and_enrich() — second validation pass + error columns
       ↓
Split into df_valid and df_invalid
       ↓
┌─ df_valid ──────────────────────────────────────────────┐
│  Stream 1: write raw valid trades → Parquet           │
│  Stream 2: dedup by event_id → watermark →            │
│            aggregate 1m OHLC → upsert Postgres        │
└───────────────────────────────────────────────────────┘
┌─ df_invalid ──────────────────────────────────────────┐
│  Stream 3: wrap as DLQ envelope → Kafka DLQ topic   │
└───────────────────────────────────────────────────────┘
```

Each stream writes **pipeline_metrics** after every micro-batch.

### Phase C — Serving (Dashboard)

Every **30 seconds** (configurable via `DASHBOARD_REFRESH_SECONDS`):

```text
Streamlit queries PostgreSQL
       ↓
fetch_candles(symbol) → Plotly candlestick chart
fetch_pipeline_metrics() → throughput, DLQ counts
fetch_dlq_summary() → recent rejection reasons
```

---

## Part 3: Tracing one trade (detailed)

Let's follow a single BTC trade.

### 1. Binance sends raw WebSocket message

```json
{
  "stream": "btcusdt@trade",
  "data": {
    "s": "BTCUSDT",
    "t": 5123456789,
    "p": "67432.15000000",
    "q": "0.00500000",
    "T": 1712345678901,
    "m": false
  }
}
```

### 2. Producer normalizes it

`binance_ws.py` produces:

```json
{
  "event_id": "BTCUSDT-5123456789-1712345678901",
  "symbol": "BTCUSDT",
  "trade_id": 5123456789,
  "price": "67432.15000000",
  "quantity": "0.00500000",
  "quote_qty": "337.16075000",
  "trade_time_ms": 1712345678901,
  "is_buyer_maker": false,
  "ingested_at_ms": 1712345679123,
  "source": "binance_ws"
}
```

### 3. Producer validates

`validate_trade()` checks:

- Price is numeric and > 0 ✓
- Quantity is numeric and >= 0 ✓
- Symbol is in `SYMBOLS` ✓
- `trade_time_ms` within ±24h of now ✓

→ Published to `crypto_trades` with Kafka key `BTCUSDT`.

### 4. Spark reads from Kafka

In the next micro-batch (within ~5 seconds), Spark:

1. Parses the JSON into columns
2. Re-validates (second pass — catches anything that slipped through)
3. Marks `is_valid = true`

### 5. Raw Parquet write

The valid row is appended to:

```text
data/raw/trades/year=2024/month=4/day=5/hour=14/part-00000.parquet
```

### 6. Dedup + watermark + aggregate

- `dropDuplicates(["event_id"])` — if this trade was already seen, skip
- `withWatermark("trade_time", "2 minutes")` — allow 2 min lateness
- `groupBy(symbol, 1-minute window)` → OHLC candle

For this trade, it contributes to the candle for window `14:32:00 – 14:33:00 UTC`:

| Field | Value |
|-------|-------|
| open_price | depends on first trade in window |
| close_price | this trade's price (if last in window) |
| high_price | max in window |
| low_price | min in window |
| volume | sum of quantities |
| trade_count | count of trades |

### 7. PostgreSQL upsert

```sql
INSERT INTO analytics.candles_1m (symbol, window_start, ...)
VALUES ('BTCUSDT', '2024-04-05 14:32:00+00', ...)
ON CONFLICT (symbol, window_start) DO UPDATE SET ...
```

If the candle for that minute already exists (from earlier trades in the same window), it **updates** rather than duplicates.

### 8. Dashboard displays

Streamlit fetches the candle row and renders a Plotly candlestick chart.

**Total latency:** roughly 5–30 seconds from trade execution to visible chart (trigger interval + watermark + dashboard refresh).

---

## Part 4: The batch workflow (scheduled)

Batch jobs run on a schedule via Airflow. DAGs are **paused by default** — enable or trigger manually.

### Workflow A — Hourly REST backfill (`crypto_hourly_backfill`)

**Schedule:** every hour  
**Purpose:** Fill gaps when WebSocket missed trades

```text
For each symbol in SYMBOLS:
  Call Binance REST /api/v3/aggTrades (last 2 hours by default)
       ↓
  normalize_agg_trade() for each row
       ↓
  validate_backfill_rows() using shared validation
       ↓
  Write valid rows to raw Parquet (same path as streaming)
       ↓
  Log pipeline_metrics (valid count, rejected count)
```

Rejected rows are counted but not written (backfill does not publish to Kafka DLQ).

### Workflow B — Daily batch (`crypto_daily_batch`)

**Schedule:** once per day  
**Purpose:** Compact storage + compute daily summaries

```text
resolve_trade_date() → yesterday (or specified date)
       ↓
compact_day():
  Read raw Parquet for that date
  dropDuplicates(event_id)
  coalesce to fewer files
  Write to data/raw/trades_compacted/
       ↓
run_daily_summary():
  Read compacted (or raw) Parquet for that date
  Compute per-symbol: VWAP, total_volume, high, low
  Upsert into analytics.daily_summary
       ↓
Log pipeline_metrics for each step
```

If no data exists for the date → status `skipped_no_data` (graceful, not a crash).

### Workflow C — Data quality (`crypto_data_quality`)

**Schedule:** once per day  
**Purpose:** Fail if too much bad data entered the system

```text
Query pipeline_metrics for last 24 hours
  SUM(records_in), SUM(records_dlq)
       ↓
compute_dlq_ratio() = records_dlq / records_in
       ↓
Compare to thresholds:
  >= 1% (DLQ_WARN_RATIO) → warn (log warning)
  >= 5% (DLQ_FAIL_RATIO) → fail (raise RuntimeError, DAG fails)
```

---

## Part 5: What happens when things go wrong

### Bad trade (empty price)

```text
Producer → validate fails → DLQ envelope → dead_letter_queue
Spark never sees it on crypto_trades
```

### Bad trade slips to Kafka (manual injection)

```text
Spark validate_and_enrich → is_valid=false
Spark invalid stream → DLQ Kafka topic
pipeline_metrics.records_dlq incremented
```

### Duplicate event_id

```text
Spark dedup_and_watermark → dropDuplicates
Only first occurrence counts toward candle volume
```

### Producer stops for 2 minutes

```text
Kafka buffers trades (7-day retention)
Spark checkpoints preserve state
Producer restarts → resumes publishing
Spark catches up from Kafka offset
No duplicate candles (dedup + upsert)
```

### Spark streaming stops for 1 minute

```text
Kafka accumulates messages
On restart: reads from checkpoint offset
Processes backlog in micro-batches
Dashboard catches up after lag clears
```

### Full stack restart (`docker compose down` then `up`)

```text
Postgres data persists (postgres_data volume)
Parquet + checkpoints persist (./data mount)
Kafka offsets depend on checkpoint + STARTING_OFFSETS
Topics recreated by kafka-init if needed
```

**Destructive reset:** `docker compose down -v` deletes volumes.

---

## Part 6: Manual verification workflow

After starting the stack, verify each layer:

| Step | Command / Action | Expected |
|------|------------------|----------|
| 1 | `docker compose ps` | All services running |
| 2 | Kafka consumer on `crypto_trades` | JSON trade events |
| 3 | `psql` query on `candles_1m` | Rows with recent window_start |
| 4 | Open dashboard :8501 | Candlestick charts |
| 5 | Trigger `crypto_hourly_backfill` | New Parquet files, metrics row |
| 6 | Trigger `crypto_daily_batch` | `daily_summary` rows |
| 7 | `make test` | All unit tests pass |

---

## Timeline summary

```text
T+0s     make up — containers start
T+30s    Kafka topics ready, producer connects
T+60s    First trades in Kafka
T+65s    First Spark micro-batch
T+90s    First candles in PostgreSQL
T+120s   Dashboard shows charts
T+1h     Airflow triggers hourly backfill (if enabled)
T+24h    Airflow triggers daily batch + DQ (if enabled)
```

---

## Summary

| Workflow | Trigger | Input | Output |
|----------|---------|-------|--------|
| Live ingestion | Continuous | Binance WebSocket | Kafka `crypto_trades` |
| Stream processing | Every 5s | Kafka | Parquet + `candles_1m` + metrics |
| Hourly backfill | Airflow `@hourly` | Binance REST | Raw Parquet |
| Daily batch | Airflow `@daily` | Raw Parquet | Compacted Parquet + `daily_summary` |
| Data quality | Airflow `@daily` | `pipeline_metrics` | Pass/warn/fail |
| Dashboard | Every 30s | PostgreSQL | Charts + health |

Next: [Tech Stack](./tech-stack.md) — detailed look at every technology and why it's used.
