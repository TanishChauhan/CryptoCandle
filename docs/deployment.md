# Deployment

How to **install, configure, run, verify, and troubleshoot** this project on your local machine. Written step-by-step for beginners.

---

## Prerequisites

| Requirement | Minimum | Notes |
|-------------|---------|-------|
| Docker Desktop | Latest | Must include Docker Compose |
| Python | 3.11+ | For local unit tests only |
| RAM | 8 GB recommended | Kafka + Spark + Airflow are memory-hungry |
| Disk | ~2 GB free | For Docker images + data |
| OS | Windows, macOS, Linux | Windows users: WSL2 backend recommended |

---

## Step 1: Clone and configure

```bash
git clone <your-repo-url>
cd crypto-codebase-ETL
cp .env.example .env
```

The `.env` file controls all services. Key variables:

| Variable | Default | What it controls |
|----------|---------|------------------|
| `SYMBOLS` | `BTCUSDT,ETHUSDT,SOLUSDT` | Which pairs to track |
| `WATERMARK_MINUTES` | `2 minutes` | Spark lateness window |
| `STREAM_TRIGGER_SECONDS` | `5` | Micro-batch interval |
| `DLQ_WARN_RATIO` | `0.01` (1%) | DQ warning threshold |
| `DLQ_FAIL_RATIO` | `0.05` (5%) | DQ failure threshold |
| `HOST_DATA_DIR` | `./data` | Where Parquet/checkpoints live on your machine |
| `BACKFILL_LOOKBACK_HOURS` | `2` | How far back hourly backfill reaches |
| `DASHBOARD_REFRESH_SECONDS` | `30` | Dashboard auto-refresh interval |

You usually don't need to change defaults for a first run.

---

## Step 2: Install Python dependencies (for tests)

```bash
pip install -e ".[dev]"
```

This installs:
- Core: pydantic, structlog, confluent-kafka, websockets, httpx
- Dev: pytest, ruff

Spark and dashboard deps are only needed inside Docker containers.

---

## Step 3: Run unit tests (optional but recommended)

```bash
make test
# or: pytest
```

Expected: all tests pass. This confirms your Python environment works before starting Docker.

```bash
make lint
# or: ruff check shared tests services dashboard
```

---

## Step 4: Start the full stack

```bash
make up
# equivalent to: docker compose --env-file .env up -d
```

Docker will:
1. Pull images (first run takes several minutes)
2. Build custom images (producer, spark-streaming, airflow, dashboard)
3. Start services in dependency order
4. Run one-shot init containers (kafka-init, postgres-schema, airflow-init)

### Check status

```bash
make ps
# or: docker compose ps
```

All services should show `running` or `exited (0)` for one-shot containers.

### Watch logs

```bash
make logs
# or: docker compose logs -f producer spark-streaming
```

Look for:
- Producer: `ws_connected`, `producer_metrics`
- Spark: streaming query started messages

---

## Step 5: Verify services

### Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Streamlit Dashboard | http://localhost:8501 | None |
| Airflow UI | http://localhost:8081 | admin / admin |
| PostgreSQL | localhost:5432 | crypto / crypto_secret |
| Kafka (host) | localhost:9092 | None |

### Verify Kafka has messages

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic crypto_trades \
  --from-beginning \
  --max-messages 3
```

You should see JSON trade events within 1–2 minutes of startup.

### Verify PostgreSQL has candles

```bash
docker compose exec postgres psql -U crypto -d crypto_analytics -c \
  "SELECT symbol, window_start, close_price, volume FROM analytics.candles_1m ORDER BY window_start DESC LIMIT 5;"
```

### Verify dashboard

Open http://localhost:8501 → select a symbol → candlestick chart should appear.

---

## Step 6: Enable Airflow DAGs

DAGs are **paused at creation**. Either:

### Option A: Airflow UI

1. Open http://localhost:8081 (login: admin / admin)
2. Wait 3–5 minutes if page is blank (webserver may be slow on 8 GB RAM)
3. Toggle each DAG ON (crypto_hourly_backfill, crypto_daily_batch, crypto_data_quality)
4. Click "Trigger DAG" for manual first run

### Option B: CLI (recommended on low RAM)

```powershell
.\scripts\airflow-trigger.ps1 crypto_hourly_backfill
.\scripts\airflow-trigger.ps1 crypto_daily_batch
.\scripts\airflow-trigger.ps1 crypto_data_quality
```

Or directly:

```bash
docker compose exec airflow-scheduler airflow dags trigger crypto_hourly_backfill
docker compose exec airflow-scheduler airflow dags list-runs -d crypto_hourly_backfill
```

---

## Docker services reference

| Service | Image | Port | Restart policy |
|---------|-------|------|----------------|
| zookeeper | confluentinc/cp-zookeeper:7.6.0 | — | default |
| kafka | confluentinc/cp-kafka:7.6.0 | 9092 | default |
| kafka-init | confluentinc/cp-kafka:7.6.0 | — | one-shot |
| postgres | postgres:16-alpine | 5432 | default |
| postgres-schema | postgres:16-alpine | — | one-shot |
| producer | custom build | — | unless-stopped |
| spark-streaming | custom build | — | unless-stopped |
| airflow-init | custom build | — | one-shot |
| airflow-webserver | custom build (slim) | 8081 | unless-stopped |
| airflow-scheduler | custom build | — | default |
| dashboard | custom build | 8501 | default |

---

## Volume mounts

| Host path | Container path | Contents |
|-----------|----------------|----------|
| `./data` | `/data` | Parquet files + Spark checkpoints |
| `postgres_data` (Docker volume) | `/var/lib/postgresql/data` | Database files |
| `./airflow/dags` | `/opt/airflow/dags` | DAG files (live reload) |
| `./dashboard` | `/app/dashboard` | Dashboard code (live reload) |

---

## Makefile commands

| Command | What it does |
|---------|-------------|
| `make install` | `pip install -e .` |
| `make dev-install` | `pip install -e ".[dev]"` |
| `make test` | Run pytest |
| `make lint` | Run ruff linter |
| `make up` | Start Docker stack |
| `make down` | Stop Docker stack |
| `make ps` | Show service status |
| `make logs` | Follow producer + spark-streaming logs |
| `make db-init` | Re-apply analytics schema to existing Postgres volume |

---

## Stopping the stack

```bash
make down
# or: docker compose down
```

Data persists:
- PostgreSQL data (Docker volume `postgres_data`)
- Parquet + checkpoints (`./data/` folder)

### Destructive reset (deletes all data)

```bash
docker compose down -v
rm -rf data/raw data/checkpoints
docker compose up -d
```

---

## Troubleshooting

### No Kafka messages

| Cause | Fix |
|-------|-----|
| Producer not running | `docker compose logs producer` |
| WebSocket disconnect | Wait for auto-reconnect (exponential backoff) |
| Wrong symbols | Check `SYMBOLS` in `.env` |

### Spark not writing candles

| Cause | Fix |
|-------|-----|
| Checkpoint corruption | Delete `data/checkpoints/stream_trades/` and restart |
| All events invalid | Check `pipeline_metrics` for high `records_dlq` |
| Postgres tables missing | Run `make db-init` |

### Empty dashboard

| Cause | Fix |
|-------|-----|
| No data yet | Wait 1–2 min after producer + spark start |
| Postgres not ready | `docker compose ps postgres` — must be healthy |
| Schema not applied | `make db-init` |

### Airflow UI blank (ERR_EMPTY_RESPONSE)

| Cause | Fix |
|-------|-----|
| Webserver OOM on 8 GB RAM | Wait 3–5 min; use slim webserver image (already configured) |
| Still failing | Trigger DAGs via CLI instead of UI |

### Windows `./data` permission errors

| Cause | Fix |
|-------|-----|
| Bind mount permissions | Pre-create `data/` folder; use WSL2 path |

### Docker memory on WSL2

Create `%UserProfile%\.wslconfig`:

```ini
[wsl2]
memory=10GB
```

Then run `wsl --shutdown` and restart Docker Desktop.

### `relation "analytics.candles_1m" does not exist`

Postgres volume was created before `init.sql` ran:

```bash
make db-init
```

### `WATERMARK_MINUTES=10` Spark error

Must include unit. Fixed automatically by `shared/watermark.py` — use `10` or `10 minutes`.

---

## Resource usage tips

| Tip | Why |
|-----|-----|
| Don't run Airflow webserver if only testing streaming | Saves ~500 MB RAM |
| Use CLI to trigger DAGs | Avoids webserver OOM |
| Limit symbols to 1–2 for testing | Reduces Kafka/Spark load |
| Set `LOG_LEVEL=WARNING` | Less log noise |

---

## Deployment architecture (local)

```text
Your machine
├── Docker Desktop
│   └── crypto-net (bridge network)
│       ├── 11 containers (see table above)
│       └── postgres_data volume
├── ./data/ (bind mount)
│   ├── raw/trades/         ← Parquet
│   ├── raw/trades_compacted/
│   └── checkpoints/
└── Python venv (optional, for tests)
```

This is **not** a cloud deployment. To deploy to AWS/GCP/Azure you would typically:

- Replace Docker Compose with Kubernetes/ECS/Cloud Run
- Use managed Kafka (MSK, Confluent Cloud)
- Use managed Postgres (RDS, Cloud SQL)
- Use S3/ADLS instead of local Parquet
- Use managed Airflow (MWAA, Cloud Composer)

The patterns and code transfer directly — only infrastructure changes.

---

## Verification checklist

After deployment, confirm each layer:

- [ ] `docker compose ps` — all services healthy
- [ ] Kafka consumer shows trade JSON
- [ ] `candles_1m` has recent rows
- [ ] Dashboard shows candlestick chart
- [ ] `data/raw/trades/` has Parquet files
- [ ] `pipeline_metrics` has rows from spark jobs
- [ ] `make test` passes
- [ ] Airflow DAGs trigger successfully (optional)

---

## Summary

| Step | Command |
|------|---------|
| Configure | `cp .env.example .env` |
| Test locally | `make test` |
| Start stack | `make up` |
| Verify | Kafka consumer + psql + dashboard |
| Schedule batch | Enable/trigger Airflow DAGs |
| Stop | `make down` |
| Reset | `docker compose down -v` + delete `data/` |

Next: [Concepts](./concepts.md) — important data engineering ideas used in this project.
