# Telecom OTP-Fraud & Analytics Pipeline

A production-grade, locally-runnable data pipeline for a telecom / CPaaS provider that
sends OTPs (SMS one-time-passwords) on behalf of enterprises. It detects **SMS/AIT
fraud** ("SMS pumping") in near real time and produces delivery/conversion analytics
from the same event stream.

## Architecture

```
Synthetic generator → Kafka → Spark Structured Streaming ┐
                                  ├─ real-time fraud rule engine → otp.alerts + Postgres
                                  └─ Bronze landing → Iceberg (MinIO/S3)
                                            │
              Airflow ──► Spark batch: Bronze→Silver ──► dbt Gold marts (Trino)
                          + daily Isolation-Forest anomaly net → Postgres
```

- **Stream engine:** Spark Structured Streaming · **Lakehouse:** Apache Iceberg on MinIO
- **Serving:** Postgres (alerts/marts) + Trino · **Orchestration:** Airflow · **Marts:** dbt

See [`docs/superpowers/specs/`](docs/superpowers/specs) for the design spec,
[`docs/SCALING.md`](docs/SCALING.md) for the petabyte-production mapping, and
[`docs/RUNBOOK.md`](docs/RUNBOOK.md) to run it.

## Fraud detection
- **Real-time rule engine** ([`streaming/rules.py`](streaming/rules.py),
  [`streaming/scoring.py`](streaming/scoring.py)): velocity, **AIT signature**
  (high volume + low verification rate), repeated-never-verified, and high-risk routing,
  combined into a `low/medium/high` severity score.
- **Offline ML net** ([`batch/anomaly.py`](batch/anomaly.py)): daily Isolation Forest
  over per-entity behavioral features catches novel patterns off the hot path.

## Quick start
```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python -m pytest -q                       # 27 tests, no Docker required
docker compose -f infra/docker-compose.yml up -d   # then follow docs/RUNBOOK.md
```

## Layout
| Path | Responsibility |
|------|----------------|
| `generator/` | synthetic OTP event producer (normal + AIT bursts) |
| `streaming/` | Bronze sink + stateful real-time fraud job + rule engine |
| `batch/` | Bronze→Silver transform, behavioral features, anomaly scoring |
| `dbt/` | Gold marts (funnel, latency, AIT loss, fraud daily) + tests |
| `airflow/` | batch ETL and anomaly DAGs |
| `quality/` | Silver data-quality validation |
| `infra/` | docker-compose (Kafka, MinIO, Trino, Postgres) + Iceberg catalog |
| `tests/` | pytest unit + end-to-end fraud-path tests |
