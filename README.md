# Subscription/SaaS Promo-Abuse Detector (Kafka → Spark → Postgres)

Detect repeated sign-ups (promo/trial abuse) within a **rolling 24-hour window** using streaming counts keyed by **device fingerprint** (or tokenized payment). End-to-end stack: **Kafka** (ingest), **Spark Structured Streaming** (windowed aggregation), **PostgreSQL** (raw facts + alerts). Fully dockerized.

## Table of Contents
- [Subscription/SaaS Promo-Abuse Detector (Kafka → Spark → Postgres)](#subscriptionsaas-promo-abuse-detector-kafka--spark--postgres)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Architecture](#architecture)
  - [Prerequisites \& System Requirements](#prerequisites--system-requirements)
    - [Required](#required)
    - [Recommended machine](#recommended-machine)
  - [Quick Start (5 steps)](#quick-start-5-steps)
  - [Detailed Setup](#detailed-setup)
    - [1) Environment](#1-environment)
    - [2) Database schema](#2-database-schema)
    - [3) Producer (simulator)](#3-producer-simulator)
    - [4) Spark streaming job](#4-spark-streaming-job)
  - [Validating the Pipeline](#validating-the-pipeline)
  - [Configuration](#configuration)
  - [Why this Architecture](#why-this-architecture)
  - [Troubleshooting](#troubleshooting)
    - [Postgres errors](#postgres-errors)
    - [Kafka / Producer issues](#kafka--producer-issues)
    - [Spark submission](#spark-submission)
    - [“No data in alerts”](#no-data-in-alerts)
    - [Notebook specifics](#notebook-specifics)
  - [Operational Notes](#operational-notes)

---

## Overview
**Problem:** The same person (or small ring) creates multiple “new” accounts to re-use promos/free trials, wasting spend and skewing KPIs.  
**Solution:** Streaming job counts sign-ups per stable key (e.g., `device_id`) over **24 hours** and emits an **alert** when the count crosses a threshold (default **3**).  
**Outputs:**  
- `signups` (raw events for audit/ML)  
- `alerts` (actionable, explainable records with window bounds, counts, first/last seen)

---

## Architecture

```mermaid
flowchart LR
  subgraph Producer (Sim / Real App)
    A[Signup Event \n {user_id, device_id, timestamp, ...}]
  end

  subgraph Kafka
    K[(Topic: signup-events)]
  end

  subgraph Spark Structured Streaming
    S1[Parse + watermark(24h)]
    S2[Window(ts, 24h) \n groupBy(key).count()]
    S3{count >= THRESHOLD}
  end

  subgraph Postgres
    P1[(signups)]
    P2[(alerts)]
  end

  A --> K
  K --> S1 --> S2 --> S3
  S1 --> P1
  S3 --> P2
```

**Key choices**
- **Kafka** → durable buffer & per-key ordering  
- **Spark** → stateful windowing + watermarking + checkpointing  
- **Postgres** → simple SQL, JSONB raw storage, easy dashboards/integration

---

## Prerequisites & System Requirements

### Required
- **Docker & Docker Compose**
  - Docker Desktop ≥ 4.x (Windows/Mac) or Docker Engine ≥ 24 (Linux)
- **Make sure ports are free:** `5432` (Postgres), `9092/9094` (Kafka broker inside/host)
- **Python 3.9+** (only if running the local producer or notebook)

### Recommended machine
- 4 vCPU, 8 GB RAM (minimum workable: 2 vCPU, 4 GB RAM)

---

## Quick Start (5 steps)

```bash
# 1) Bring up the stack
docker compose up -d

# 2) (Optional) Tail service health
docker ps
docker inspect -f '{{.State.Health.Status}}' postgres kafka zookeeper spark

# 3) Start the producer from the host (sends signups to localhost:9094)
python -m venv .venv && source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r producer/requirements.txt
python producer/signup_producer.py

# 4) Start the Spark streaming job in the container (includes Kafka + JDBC drivers)
bash scripts/run_spark.sh

# 5) Verify data landing in Postgres
docker exec -it postgres psql -U app -d fraud -c "\dt"
docker exec -it postgres psql -U app -d fraud -c "SELECT * FROM alerts ORDER BY created_at DESC LIMIT 5;"
```

Stop everything:
```bash
docker compose down
```

---

## Detailed Setup

### 1) Environment
The repo includes a `.env` used by `docker-compose.yml`:

```
POSTGRES_DB=fraud
POSTGRES_USER=app
POSTGRES_PASSWORD=app
KAFKA_ADVERTISED_LISTENER_HOST=localhost
```

> **Note:** Kafka exposes **9094** to the host (for your local producer) and **9092** inside the Docker network (for Spark).

### 2) Database schema
`sql/init.sql` runs automatically at container start and creates:
- `signups(id, event, user_id, device_id, ts, label_abusive, raw_json JSONB, ingested_at)`
- `alerts(id, device_id, window_start, window_end, signup_count, threshold, first_seen_ts, last_seen_ts, created_at)`

### 3) Producer (simulator)
- File: `producer/signup_producer.py`
- Topic: `signup-events`
- Key: `device_id` (ensures ordered processing per device)
- Default bootstrap: `localhost:9094`

Run:
```bash
pip install -r producer/requirements.txt
python producer/signup_producer.py
```

### 4) Spark streaming job
- File: `stream/main_stream.py`
- Reads from Kafka **inside the network** (`kafka:9092`)
- Writes:
  - raw → `signups`
  - alerts (threshold ≥ 3, 24h window) → `alerts`
- Checkpoints:
  - `/opt/stream/checkpoints/signups`
  - `/opt/stream/checkpoints/alerts`

Submit:
```bash
bash scripts/run_spark.sh
```

---

## Validating the Pipeline

A few ready-to-run SQLs:

```sql
-- Show latest signups
SELECT user_id, device_id, ts, label_abusive
FROM signups
ORDER BY ts DESC
LIMIT 10;

-- Recent alerts with evidence
SELECT device_id, window_start, window_end, signup_count, threshold,
       first_seen_ts, last_seen_ts, created_at
FROM alerts
ORDER BY created_at DESC
LIMIT 10;

-- Optional: approximate alert latency (DB write - first seen)
SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (created_at - first_seen_ts)))
AS p95_seconds
FROM alerts;
```

Run via Docker:
```bash
docker exec -it postgres psql -U app -d fraud -c "<SQL HERE>"
```

---

## Configuration

| Item | Where | Default | Notes |
|---|---|---:|---|
| Kafka topic | code + producer | `signup-events` | Create explicitly if auto-create is disabled |
| Key by | producer | `device_id` | Can swap to payment token |
| Window length | `stream/main_stream.py` | 24 hours | `window(col("ts"), "24 hours")` |
| Watermark | `stream/main_stream.py` | 24 hours | Bounds state; tolerates late events |
| Alert threshold | `stream/main_stream.py` | 3 | Change `THRESHOLD` constant |
| Postgres JDBC URL | `stream/main_stream.py` | `jdbc:postgresql://postgres:5432/fraud` | Container hostname `postgres` |
| Checkpoints | `stream/main_stream.py` | `/opt/stream/checkpoints/...` | Do **not** delete while job is running |

**Optional duplicate suppression (DB-level):**
```sql
CREATE UNIQUE INDEX IF NOT EXISTS uniq_alerts_device_window
ON alerts(device_id, window_start, window_end);
```

---

## Why this Architecture

- **Kafka** → decouples producers/consumers, preserves per-key order, smooths spikes.  
- **Spark Structured Streaming** → native windowing + watermarking + checkpointing for **exactly-once progress** and low-latency micro-batches.  
- **PostgreSQL** → simple ops surface, JSONB raw capture for audit/ML, easy dashboards/BI.

This directly maps to the problem: **count per device/payment over 24h** → **emit explainable alert** → **persist facts & alerts**.

---

## Troubleshooting

### Postgres errors
- **`role "app" does not exist` / auth failures**  
  Container may not have initialized. Recreate DB container to re-run `init.sql`:
  ```bash
  docker compose rm -sf postgres && docker compose up -d postgres
  docker logs -f postgres   # wait for "database system is ready"
  ```
- **Port 5432 in use**  
  Stop local Postgres or change port mapping in `docker-compose.yml`.

### Kafka / Producer issues
- **Producer cannot connect (`localhost:9094`)**  
  Ensure broker started and host listener is exposed:
  ```bash
  docker logs kafka | tail -n 200
  ```
  If using WSL2/VMs, `localhost` can be tricky—export `KAFKA_ADVERTISED_LISTENER_HOST` to your host IP and restart.
- **Topic auto-creation off**  
  Create topic manually (example 1 partition):
  ```bash
  docker exec -it kafka kafka-topics --create --topic signup-events --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
  ```

### Spark submission
- **Missing Kafka/JDBC packages**  
  Always submit via `scripts/run_spark.sh` (it adds `spark-sql-kafka` and `postgresql` JARs).  
- **Checkpoint corruption after schema/code change**  
  Stop job, **delete the specific checkpoint dir** for the affected sink, then restart:
  ```bash
  docker exec -it spark rm -rf /opt/stream/checkpoints/alerts
  ```

### “No data in alerts”
- The threshold might not be hit. The simulator bursts on a few devices; keep it running 30–60 seconds.  
- Check the event timestamps are **UTC ISO** and parsed to Spark `ts`.  
- Verify watermark/window: both set to **24 hours** in `main_stream.py`.

### Notebook specifics
- If running the IPYNB, install client deps in your kernel:
  ```bash
  pip install kafka-python psycopg2-binary python-dotenv
  ```

---

## Operational Notes

- **Stop/Start**  
  - Stop: `docker compose down`  
  - Start: `docker compose up -d`
- **Log tails**
  ```bash
  docker logs -f kafka
  docker logs -f spark
  docker logs -f postgres
  ```
- **Data retention**  
  Containers persist volumes by default. To reset **everything** (DB + checkpoints):
  ```bash
  docker compose down -v
  ```

---
