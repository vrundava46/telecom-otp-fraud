# Telecom OTP-Fraud & Analytics Pipeline — Design Spec

**Date:** 2026-06-21
**Status:** Approved (design), pending implementation plan
**Goal:** Production-grade reference data pipeline, implemented with local open-source
tooling (docker-compose), delivered as a fully working system.

---

## 1. Domain & Narrative

A telecom / CPaaS provider sends OTPs (SMS one-time-passwords) on behalf of
enterprise customers (banks, apps, marketplaces). At scale this is billions of
events per day. Two business problems must be served from the same event stream:

1. **Fraud detection** — primarily **AIT (Artificially Inflated Traffic)**, a.k.a.
   **"SMS pumping"**: fraudsters trigger mass OTP requests to phone-number ranges
   they profit from (they earn a cut of the SMS termination fee), with near-zero
   genuine verification. Secondary patterns: SIM-farm velocity abuse and
   OTP-retry abuse.
2. **Analytics** — delivery/conversion funnels, latency, and operator/route
   performance per enterprise, country, and operator.

This pipeline must catch fraud in (near) real time **and** produce trustworthy
historical analytics from the same source of truth.

## 2. Architecture Choices (locked)

| Concern             | Choice                          | Why |
|---------------------|---------------------------------|-----|
| Stream engine       | **Spark Structured Streaming**  | One engine for stream + batch; portfolio-legible PySpark; micro-batch latency is fine for this use case. |
| Lakehouse format    | **Apache Iceberg**              | ACID, schema evolution, time-travel, partition evolution; works cleanly with Spark + Trino + MinIO. |
| Object store        | **MinIO** (S3-compatible)       | Local stand-in for S3; same API as production. |
| Orchestration       | **Apache Airflow**              | Industry standard for batch DAGs and data-quality scheduling. |
| Transform/marts     | **dbt** (on Trino/Postgres)     | Declarative gold marts with built-in tests and lineage. |
| Serving / query     | **Postgres** + **Trino**        | Postgres serves alerts & marts to BI; Trino queries the lakehouse directly. |
| Messaging           | **Apache Kafka** (+ Schema Registry, Kafka UI) | Durable event backbone with schemas. |

## 3. Event Model

Three Kafka topics, each with a JSON Schema registered in the Schema Registry.

### `otp.requests`
| field          | type      | notes |
|----------------|-----------|-------|
| request_id     | string    | UUID, correlation key across all 3 topics |
| msisdn         | string    | destination phone (E.164) |
| enterprise_id  | string    | sending customer |
| country_iso    | string    | derived from MCC |
| mcc            | string    | mobile country code |
| mnc            | string    | mobile network code (operator) |
| ip             | string    | requester IP |
| channel        | string    | sms / voice / whatsapp |
| event_ts       | timestamp | event time |

### `otp.delivery`
| field          | type      | notes |
|----------------|-----------|-------|
| request_id     | string    | FK to request |
| status         | string    | delivered / failed / expired |
| operator_latency_ms | int  | request → handset |
| event_ts       | timestamp |  |

### `otp.verification`
| field          | type      | notes |
|----------------|-----------|-------|
| request_id     | string    | FK to request |
| outcome        | string    | verified / failed / timeout |
| attempts       | int       | number of code entries |
| event_ts       | timestamp |  |

## 4. Data Flow (medallion lakehouse + lambda-style)

```
Synthetic Event Generator (Python; tunable TPS; injects fraud scenarios)
        │  produces to
        ▼
   Kafka  (otp.requests / otp.delivery / otp.verification)   [Schema Registry, Kafka UI]
        │
   ┌────┴──────────────────────────────────────┐
   ▼ (speed layer)                              ▼ (raw landing)
Spark Structured Streaming                 Spark streaming sink
 - stateful velocity windows                → Bronze (Iceberg on MinIO)
 - real-time fraud RULE ENGINE
 - scores → otp.alerts topic + Postgres
        │
        ▼ (batch layer — Airflow-orchestrated Spark + dbt)
Bronze ──► Silver (clean, dedupe, join req+delivery+verify, enrich MCC/MNC, geo)
                 └─► Gold marts (dbt): delivery funnel, latency pctiles,
                     AIT loss estimates, operator/route perf, fraud daily agg
        │
        ▼ (serving)
Postgres warehouse  +  Trino (query lakehouse)  →  alerts & marts for BI
```

## 5. Fraud Detection

### 5.1 Streaming rule engine (primary, near real-time, stateful)
Sliding-window stateful aggregation in Spark Structured Streaming. Each rule emits
a weighted contribution to a severity score; the combined score is thresholded into
`low / medium / high` and written to the `otp.alerts` topic and a Postgres
`fraud_alerts` table.

Rules:
- **Velocity** — too many requests per `msisdn`, per `ip`, per `enterprise_id`,
  or per destination number-range within a sliding window.
- **AIT signature** — high request volume to a number-range / operator combined
  with an abnormally **low verification rate** over the window (the defining AIT
  tell: traffic that is requested but never genuinely verified).
- **Repeated-never-verified** — many requests for an msisdn with zero verifications.
- **High-risk routing** — known high-risk MCC/MNC and premium ranges.

### 5.2 Batch ML net (secondary, offline)
Daily Spark batch job builds per-entity behavioral features (volume, verify-rate,
latency, spread across ranges) and scores them with an **Isolation Forest** anomaly
model. Anomalies land in a `fraud_anomalies` Gold mart. Rules are the real-time line
of defense; ML is the offline catch-net for novel patterns.

## 6. Analytics (Gold marts via dbt)
- **Delivery funnel** — requested → delivered → verified, with conversion rates by
  enterprise / country / operator / time bucket.
- **Latency percentiles** — request→delivery and delivery→verify p50/p90/p99.
- **AIT loss estimate** — suspected-fraud volume × per-message cost, by route.
- **Operator/route performance** — delivery success and latency by MNC.
- **Fraud daily aggregate** — alert counts and severities by dimension.

## 7. Repo Layout (each unit independently testable)
```
telecom-otp-fraud/
  generator/      synthetic event producer (normal + fraud scenarios, tunable TPS)
  streaming/      PySpark structured-streaming jobs + fraud rule-engine module
  batch/          PySpark bronze→silver jobs + ML scoring
  dbt/            gold marts + dbt tests
  airflow/        DAGs: batch ETL, daily fraud agg, data-quality
  quality/        Great Expectations / data-quality checks
  infra/          docker-compose + Iceberg catalog + service configs
  tests/          pytest for rules & transforms (small in-memory DataFrames)
  docs/           specs, SCALING.md, runbook
```

## 8. Testing & Quality
- **pytest** unit tests for every fraud rule and every transformation, using small
  in-memory Spark DataFrames (deterministic, no cluster needed).
- **dbt tests** (not_null, unique, relationships, accepted_values) on marts.
- **Great Expectations** suite on the Silver layer.
- **End-to-end smoke test**: generator → Kafka → streaming → an alert row appears in
  Postgres for an injected AIT scenario.

## 9. "Production Blueprint" Mapping (`docs/SCALING.md`)
Document how each local choice maps to petabyte-scale production:
Kafka partition sizing & retention, Spark parallelism/shuffle/checkpointing,
Iceberg partitioning + compaction + retention, MinIO → S3, Postgres → Redshift/
Snowflake, Airflow → MWAA, and where autoscaling/cost controls apply.

## 10. Prerequisites
- **Docker Desktop** (not yet installed — step 0 of implementation).
- Python 3.11 and Java 21 are already available locally.

## 11. Out of Scope (YAGNI)
- Real telecom carrier integration (synthetic data only).
- BI dashboard tooling (marts are queryable; visualization is left to the consumer).
- Real-time ML inference in the stream (ML stays offline/batch by design).
- Multi-region / DR topology (covered conceptually in SCALING.md only).
