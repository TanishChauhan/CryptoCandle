# Data Flow

This document explains **how data moves, changes shape, and lands in storage** at every stage. Think of it as the "plumbing diagram" of the pipeline.

---

## The canonical data shape: TradeEvent

Every stage converges on (or diverges from) this structure:

| Field | Type | Meaning |
|-------|------|---------|
| `event_id` | string | Unique ID: `{symbol}-{trade_id}-{trade_time_ms}` |
| `symbol` | string | Trading pair, e.g. `BTCUSDT` |
| `trade_id` | int | Binance trade identifier |
| `price` | string | Trade price (string to avoid float rounding) |
| `quantity` | string | Trade quantity |
| `quote_qty` | string? | price × quantity (optional) |
| `trade_time_ms` | int | Exchange timestamp (epoch milliseconds) |
| `is_buyer_maker` | bool? | Was the buyer the market maker? |
| `ingested_at_ms` | int? | When our pipeline received it |
| `source` | string | `binance_ws` or `binance_rest` |

---

## High-level data flow map

```text
┌─────────────┐     ┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   Binance   │────▶│   Producer  │────▶│    Kafka     │────▶│    Spark    │
│  (source)   │     │  (ingest)   │     │  (buffer)    │     │  (transform)│
└─────────────┘     └─────────────┘     └──────────────┘     └──────┬──────┘
                           │                    ▲                     │
                           │ invalid            │ invalid             │
                           ▼                    │                     ├──▶ Parquet (raw)
                    ┌─────────────┐            │                     ├──▶ PostgreSQL (candles)
                    │     DLQ     │◀───────────┘                     └──▶ Kafka (DLQ)
                    │   (Kafka)   │
                    └─────────────┘
```

---

## Stage 1: Source → Producer

### Input format (Binance WebSocket)

Binance sends wrapped messages:

```json
{
  "stream": "btcusdt@trade",
  "data": {
    "s": "BTCUSDT",
    "t": 5123456789,
    "p": "67432.15",
    "q": "0.005",
    "T": 1712345678901,
    "m": false
  }
}
```

### Transformation (normalize)

`binance_ws.normalize_trade_payload()` maps Binance field names to TradeEvent:

| Binance field | TradeEvent field |
|---------------|------------------|
| `data.s` | `symbol` (uppercased) |
| `data.t` | `trade_id` |
| `data.p` | `price` (as string) |
| `data.q` | `quantity` (as string) |
| `data.T` | `trade_time_ms` |
| `data.m` | `is_buyer_maker` |
| (computed) | `event_id` |
| (computed) | `ingested_at_ms` |
| (constant) | `source = "binance_ws"` |

### Gate (validate)

`shared/validation.validate_trade()` — returns `ValidationResult`:

- **Pass** → `is_valid=True`, `event=TradeEvent(...)`
- **Fail** → `is_valid=False`, `issue=ValidationIssue(code, message)`

### Output routing

| Result | Destination | Format |
|--------|-------------|--------|
| Valid | Kafka `crypto_trades` | TradeEvent JSON, key=symbol |
| Invalid | Kafka `dead_letter_queue` | DLQ envelope JSON |

### DLQ envelope shape

```json
{
  "original_payload": { "...original trade..." },
  "error_code": "NULL_PRICE",
  "error_message": "price cannot be null",
  "field": "price",
  "failed_at_ms": 1712345679999,
  "stage": "producer",
  "symbol": "BTCUSDT"
}
```

---

## Stage 2: Kafka → Spark (read)

### Kafka message structure (Spark's view)

Spark reads Kafka with the built-in `kafka` format:

| Column | Content |
|--------|---------|
| `key` | Symbol bytes |
| `value` | TradeEvent JSON bytes |
| `topic` | `crypto_trades` |
| `partition` | 0, 1, or 2 |
| `offset` | Sequential offset |
| `timestamp` | Kafka broker timestamp |

### Parse step

`parse_kafka_trades()`:

```text
value (bytes) → cast to string → from_json(schema) → flat columns
```

Also keeps `raw_value` (original JSON string) for DLQ audit.

---

## Stage 3: Spark validation (second pass)

`validate_and_enrich()` adds columns:

| New column | Meaning |
|------------|---------|
| `price_dec` | price cast to Decimal(20,8) |
| `quantity_dec` | quantity cast to Decimal(38,12) |
| `quote_qty_dec` | quote_qty as decimal |
| `trade_time` | timestamp from trade_time_ms |
| `is_valid` | true if no error |
| `error_code` | e.g. `NULL_PRICE`, `LATE_EVENT` |
| `error_message` | Human-readable reason |

### Error code priority (first match wins)

1. `NULL_PRICE`
2. `INVALID_PRICE`
3. `NULL_QUANTITY`
4. `NEGATIVE_QUANTITY`
5. `INVALID_TIMESTAMP`
6. `LATE_EVENT`
7. `INVALID_SYMBOL`

### Split

```text
df_valid   = is_valid == true
df_invalid = is_valid == false
```

---

## Stage 4a: Valid path → Raw Parquet

**Important:** Raw sink receives `df_valid` **before** deduplication.

### Partitioning

```text
data/raw/trades/
  year=2024/
    month=4/
      day=5/
        hour=14/
          part-00000-....parquet
```

### Columns written

All TradeEvent fields plus partition columns (`year`, `month`, `day`, `hour`).

### Write mode

`append` — new micro-batches add files, never overwrite.

---

## Stage 4b: Valid path → 1m candles → PostgreSQL

### Step 1: Dedup

```python
df_valid
  .withWatermark("trade_time", "2 minutes")
  .dropDuplicates(["event_id"])
```

### Step 2: Window aggregation

```python
groupBy(symbol, window(trade_time, "1 minute"))
  .agg(
    min_by(price, trade_time)  → open_price
    avg(price)                 → avg_price
    max(price)                 → high_price
    min(price)                 → low_price
    max_by(price, trade_time)  → close_price
    sum(quantity)              → volume
    sum(quote_qty)             → quote_volume
    count(*)                   → trade_count
    stddev(price)              → volatility
  )
```

### Step 3: Upsert to PostgreSQL

Target table: `analytics.candles_1m`

| Column | Source |
|--------|--------|
| `symbol` | groupBy key |
| `window_start` | window start timestamp |
| `window_end` | window end timestamp |
| `open_price` … `volatility` | aggregations |
| `updated_at` | auto-set on conflict |

**Primary key:** `(symbol, window_start)` — ensures one candle per symbol per minute.

---

## Stage 4c: Invalid path → DLQ Kafka

`enrich_invalid_for_dlq()` wraps invalid rows:

```text
key   = symbol
value = JSON(DLQ envelope with stage="spark")
```

Written to `dead_letter_queue` topic via Spark's Kafka sink.

---

## Stage 5: REST backfill data flow

A parallel path that **writes directly to Parquet** (skips Kafka):

```text
Binance REST /api/v3/aggTrades
       ↓
normalize_agg_trade() → TradeEvent-like dict (source="binance_rest")
       ↓
validate_backfill_rows() → split valid / rejected
       ↓
valid rows → Spark DataFrame → partitioned Parquet (same raw path)
       ↓
pipeline_metrics logged
```

Rejected rows are counted but not written to DLQ Kafka in backfill.

---

## Stage 6: Daily batch data flow

### Compaction

```text
Read:  data/raw/trades/year=.../month=.../day=.../
Write: data/raw/trades_compacted/year=.../month=.../day=.../hour=.../
Ops:   dropDuplicates(event_id) → coalesce(N files)
```

### Daily summary

```text
Read:  compacted Parquet (fallback: raw Parquet) for trade_date
       ↓
groupBy(symbol, to_date(trade_time))
  .agg(
    sum(price * qty) / sum(qty)  → vwap
    sum(qty)                     → total_volume
    max(price)                   → high_price
    min(price)                   → low_price
  )
       ↓
Upsert: analytics.daily_summary (PK: trade_date, symbol)
```

---

## Stage 7: Metrics data flow

Every processing step writes to `analytics.pipeline_metrics`:

| Field | Meaning |
|-------|---------|
| `recorded_at` | When the batch finished |
| `job_name` | e.g. `spark_valid_stream`, `batch_compact_parquet` |
| `records_in` | Input count |
| `records_out` | Output count |
| `records_valid` | Valid records processed |
| `records_dlq` | Records sent to DLQ |
| `batch_duration_ms` | How long the batch took |
| `status` | `ok`, `skipped_no_data`, etc. |

### Data quality reads metrics

```text
SUM(records_in) and SUM(records_dlq) over last 24h
  → dlq_ratio = dlq / in
  → compare to DLQ_WARN_RATIO (1%) and DLQ_FAIL_RATIO (5%)
```

---

## Stage 8: Dashboard data flow

```text
PostgreSQL
  ├── analytics.candles_1m      → candlestick + volume charts
  ├── analytics.daily_summary   → daily VWAP table
  ├── analytics.pipeline_metrics → throughput, duration, DLQ bars
  └── analytics.dlq_events      → recent error codes (if populated)
       ↓
pandas DataFrame
       ↓
Plotly figures
       ↓
Streamlit rendered HTML
```

---

## Data format evolution summary

```text
Stage              Format                    Schema
─────────────────────────────────────────────────────────
Binance WS         Binance-native JSON       {s, t, p, q, T, m}
Producer output    TradeEvent JSON           shared/schema.py
Kafka              TradeEvent JSON (bytes)   same
Spark parsed       DataFrame columns         typed (String, Long, Decimal)
Raw Parquet        Parquet columns           TradeEvent + partitions
Candles            DataFrame → SQL rows      OHLC + volume + volatility
Daily summary      DataFrame → SQL rows      vwap, total_volume, high, low
DLQ                Envelope JSON             shared/dlq.py
Metrics            SQL rows                  pipeline_metrics table
Dashboard          pandas DataFrame          queried columns
```

---

## Data volume and partitioning strategy

| Store | Growth rate | Partition key | Retention |
|-------|-------------|---------------|-----------|
| Kafka `crypto_trades` | ~100s trades/sec (3 symbols) | topic partitions | 7 days |
| Kafka DLQ | Low (hopefully) | single partition | 30 days |
| Raw Parquet | Continuous append | year/month/day/hour | Manual (local disk) |
| Postgres candles | 1 row/symbol/minute | PK (symbol, window_start) | Unbounded |
| Postgres daily | 1 row/symbol/day | PK (trade_date, symbol) | Unbounded |

---

## Idempotency guarantees

| Operation | How duplicates are handled |
|-----------|--------------------------|
| Kafka consume | Spark checkpoint tracks offsets |
| Raw Parquet append | Duplicates possible; compaction dedups |
| Candle upsert | `ON CONFLICT DO UPDATE` on (symbol, window_start) |
| Daily summary upsert | `ON CONFLICT DO UPDATE` on (trade_date, symbol) |
| Spark dedup | `dropDuplicates(["event_id"])` before aggregation |

---

## Summary diagram: one trade's full journey

```text
Binance WS JSON
  → normalize → TradeEvent
  → validate (producer) ──fail──→ DLQ
  → Kafka crypto_trades
  → Spark parse → validate (spark) ──fail──→ DLQ
  → Parquet raw (append)
  → dedup → watermark → 1m window agg
  → Postgres candles_1m (upsert)
  → Dashboard candlestick chart
```

Next: [Key Components](./key-components.md) — deep dive into each service and module.
