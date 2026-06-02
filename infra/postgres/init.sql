-- PostgreSQL schema for crypto analytics
-- Runs automatically on initial container boot.
-- Airflow DB is created by init-airflow-db.sh (CREATE DATABASE cannot run in a SQL txn).

CREATE SCHEMA IF NOT EXISTS analytics;

-- 1-minute candles and stream aggregates
CREATE TABLE IF NOT EXISTS analytics.candles_1m (
  symbol           TEXT NOT NULL,
  window_start     TIMESTAMPTZ NOT NULL,
  window_end       TIMESTAMPTZ NOT NULL,
  open_price       NUMERIC(20,8),
  avg_price        NUMERIC(20,8),
  high_price       NUMERIC(20,8),
  low_price        NUMERIC(20,8),
  close_price      NUMERIC(20,8),
  volume           NUMERIC(38,12),
  quote_volume     NUMERIC(38,12),
  trade_count      BIGINT,
  volatility       NUMERIC(20,8),
  updated_at       TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (symbol, window_start)
);

-- If the table already existed from an earlier run, ensure avg_price is present.
ALTER TABLE analytics.candles_1m
ADD COLUMN IF NOT EXISTS avg_price NUMERIC(20,8);

-- Operational pipeline metrics (one row per job run/batch)
CREATE TABLE IF NOT EXISTS analytics.pipeline_metrics (
  recorded_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  job_name           TEXT NOT NULL,
  kafka_lag          BIGINT,
  batch_duration_ms BIGINT,
  records_in        BIGINT,
  records_out       BIGINT,
  records_valid     BIGINT,
  records_dlq        BIGINT,
  dlq_warn_ratio     DOUBLE PRECISION,
  dlq_fail_ratio     DOUBLE PRECISION,
  status             TEXT DEFAULT 'ok',
  PRIMARY KEY (recorded_at, job_name)
);

-- Batch-derived daily summary table
CREATE TABLE IF NOT EXISTS analytics.daily_summary (
  trade_date   DATE NOT NULL,
  symbol       TEXT NOT NULL,
  vwap          NUMERIC(20,8),
  total_volume NUMERIC(38,12),
  high_price   NUMERIC(20,8),
  low_price    NUMERIC(20,8),
  PRIMARY KEY (trade_date, symbol)
);

-- A tiny DLQ audit table (optional, but useful for dashboard)
CREATE TABLE IF NOT EXISTS analytics.dlq_events (
  received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  stage           TEXT,
  symbol          TEXT,
  error_code      TEXT,
  error_message   TEXT,
  original_payload JSONB
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_candles_1m_symbol_start ON analytics.candles_1m(symbol, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_job ON analytics.pipeline_metrics(job_name, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_dlq_events_received_at ON analytics.dlq_events(received_at DESC);
