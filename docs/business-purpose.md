# Business Purpose

## What is this project?

**Crypto Codebase ETL** is an end-to-end **crypto market analytics platform**. It takes live trade data from Binance (the world's largest crypto exchange), processes it in real time and on a schedule, stores clean results, and displays them on a dashboard.

Think of it as a **mini data platform** — the same kind of system a trading firm, analytics startup, or exchange might build internally, scaled down for learning and demonstration.

---

## The business problem

### Raw market data is messy and fast

Every second, thousands of trades happen on Binance:

- Someone buys 0.5 BTC at $67,432
- Someone sells 10 ETH at $3,450
- Prices change constantly

This raw stream is:

| Challenge | Why it matters |
|-----------|----------------|
| **High volume** | You cannot manually watch or store every trade |
| **Noisy** | Bad messages, duplicates, and late events happen |
| **Not useful as-is** | Analysts want candles, VWAP, daily summaries — not raw ticks |
| **Unreliable connections** | WebSockets disconnect; you need recovery and gap-fill |

A business needs **reliable, clean, queryable analytics** — not a firehose of JSON.

### What the business wants (outputs)

| Output | Business use |
|--------|--------------|
| **1-minute OHLC candles** | Price charts, short-term trend analysis |
| **Daily summaries (VWAP, high, low, volume)** | End-of-day reporting, portfolio review |
| **Raw trade archive (Parquet)** | Auditing, replay, machine learning, compliance |
| **Pipeline health metrics** | Know if the system is healthy before bad data spreads |
| **Rejected-record tracking (DLQ)** | Debug upstream issues without stopping the pipeline |

---

## Who is this for?

This project serves **two audiences**:

### 1. Learners (you)

You get hands-on experience with real tools used in industry:

- Kafka for event buffering
- Spark for large-scale transforms
- Airflow for scheduling
- PostgreSQL as a serving layer
- Docker for local deployment

Every design choice mirrors production patterns: validation, dead-letter queues, deduplication, watermarking, and data quality gates.

### 2. Resume / portfolio positioning

The README includes a one-liner you can use in interviews:

> Built an end-to-end hybrid batch + streaming crypto analytics platform using Kafka, PySpark, PostgreSQL, Airflow, and Docker on live Binance data — with schema validation, dead-letter queues, deduplication, watermarking, unit-tested transforms, and operational dashboards.

That sentence signals you understand **both** real-time streaming **and** scheduled batch processing — a combination many junior data engineers lack.

---

## What "business value" does each layer provide?

```text
Binance API          →  Access to real market truth
Producer + Kafka     →  Never lose events; decouple ingestion from processing
Spark Streaming      →  Low-latency candles for live dashboards
Parquet raw zone     →  Cheap, durable history for analytics and ML
Airflow batch jobs   →  Fill gaps, compact files, compute daily rollups
Data quality DAG     →  Stop silent data corruption (fail when DLQ ratio spikes)
PostgreSQL           →  Fast SQL queries for apps and dashboards
Streamlit dashboard  →  Human-readable proof the system works
```

---

## Default scope: which markets?

Out of the box, the pipeline tracks three USDT pairs:

- **BTCUSDT** — Bitcoin vs Tether
- **ETHUSDT** — Ethereum vs Tether
- **SOLUSDT** — Solana vs Tether

These are configured via the `SYMBOLS` environment variable. The business logic (validation, aggregation) is symbol-agnostic — you can add more pairs without rewriting core code.

---

## Hybrid batch + streaming: why both?

This is a key business decision, not just a tech choice.

| Mode | When it runs | Business role |
|------|--------------|---------------|
| **Streaming** | Continuously (every ~5 seconds) | Live charts, near-real-time monitoring |
| **Batch (hourly)** | REST backfill | Fill gaps when WebSocket drops or producer restarts |
| **Batch (daily)** | Compaction + daily summary | Efficient storage and end-of-day metrics |
| **Batch (daily DQ)** | Quality gate | Alert when too much bad data enters the system |

**Streaming alone** cannot guarantee completeness (network blips happen).  
**Batch alone** cannot give you live candles.  
**Together**, you get freshness **and** reliability.

---

## Fail-safe vs fail-loud

A critical business principle in this repo:

> **Bad data must not crash the pipeline.**

When a trade has an empty price, invalid symbol, or impossible timestamp:

1. The event is **rejected**
2. It goes to the **Dead Letter Queue (DLQ)**
3. The stream **keeps running**
4. Metrics record how many were rejected

This is **fail-safe** design. The alternative — crashing on every bad record — would mean downtime during market hours, which is unacceptable in finance.

The **data quality DAG** adds a **fail-loud** layer on top: if *too many* records are bad (e.g. >5% DLQ ratio), Airflow **fails the job** so humans investigate.

---

## What this project is NOT

Being clear about scope helps you learn honestly:

| Not included | Why |
|--------------|-----|
| Trading / order execution | This is analytics only, not a trading bot |
| Cloud deployment (AWS/GCP) | Runs locally via Docker Compose |
| Multi-exchange support | Only Binance in the current code |
| Real-time alerting (PagerDuty, Slack) | Metrics exist; alerting is a natural extension |
| Authentication on dashboard | Local dev setup; no login required |

These are intentional simplifications for learning. The patterns you learn here transfer directly to production systems.

---

## Success criteria (how you know it works)

From a business perspective, the system succeeds when:

1. **Live candles appear** in PostgreSQL within 1–2 minutes of starting the stack
2. **Dashboard shows OHLC charts** for BTC, ETH, SOL
3. **Raw Parquet files grow** under `data/raw/trades/`
4. **DLQ stays low** under normal operation (< 1% warn threshold)
5. **Airflow DAGs complete** without failure after manual trigger
6. **Producer restart** does not break Spark or cause duplicate candle inflation (dedup works)

---

## Summary

| Question | Answer |
|----------|--------|
| **What problem?** | Turn chaotic live crypto trades into clean, stored, visualized analytics |
| **Who benefits?** | Learners, portfolio builders, anyone studying data engineering |
| **Core outputs?** | 1m candles, daily summaries, raw archive, pipeline health |
| **Key principle?** | Hybrid streaming + batch, fail-safe ingestion, fail-loud quality gates |

Next: [Architecture](./architecture.md) — how the system is structured to deliver this value.
