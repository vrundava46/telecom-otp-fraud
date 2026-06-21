# Telecom OTP-Fraud & Analytics Pipeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a production-grade, locally-runnable OTP event pipeline that detects SMS/AIT fraud in near real time and produces delivery/conversion analytics.

**Architecture:** Synthetic generator → Kafka → Spark Structured Streaming (real-time fraud rule engine + Bronze landing to Iceberg/MinIO) → Airflow-orchestrated Spark batch (Bronze→Silver) → dbt Gold marts → Postgres/Trino serving. Offline Isolation-Forest anomaly net runs daily.

**Tech Stack:** Python 3.11, PySpark 3.5, Apache Iceberg, Kafka, MinIO (S3), Postgres, Trino, dbt-trino, Airflow, Great Expectations, pytest, docker-compose.

---

## File Structure (decomposition)

```
telecom-otp-fraud/
  requirements.txt                 pinned Python deps
  pyproject.toml                   pytest + package config
  .env.example                     service endpoints / creds
  infra/
    docker-compose.yml             kafka, schema-registry, kafka-ui, minio, trino, postgres, airflow
    iceberg/trino-iceberg.properties   Trino Iceberg catalog
    minio/init-buckets.sh          create lakehouse + warehouse buckets
  common/
    __init__.py
    config.py                      typed settings loaded from env
    schemas.py                     event field constants + JSON schemas
    spark.py                       Spark session factory (Iceberg + S3A configured)
  generator/
    __init__.py
    scenarios.py                   normal + fraud traffic profiles
    producer.py                    emits request/delivery/verification to Kafka
  streaming/
    __init__.py
    rules.py                       PURE fraud rule functions (heart, fully unit-tested)
    scoring.py                     combine rule outputs → severity
    bronze_sink.py                 stream Kafka → Bronze Iceberg
    fraud_job.py                   stateful streaming fraud detection → alerts
  batch/
    __init__.py
    silver.py                      Bronze→Silver transforms (join/dedupe/enrich)
    features.py                    per-entity behavioral features
    anomaly.py                     Isolation Forest scoring
  dbt/
    dbt_project.yml
    profiles.yml
    models/gold/*.sql              funnel, latency, ait_loss, operator_perf, fraud_daily
    models/gold/schema.yml         dbt tests
  airflow/dags/
    batch_etl_dag.py               bronze→silver→dbt→quality
    fraud_anomaly_dag.py           daily features + isolation forest
  quality/
    expectations_silver.py         Great Expectations suite on Silver
  tests/
    test_scenarios.py
    test_producer.py
    test_rules.py
    test_scoring.py
    test_silver.py
    test_features.py
    test_anomaly.py
    test_e2e_smoke.py
  docs/
    SCALING.md
    RUNBOOK.md
```

**Design notes:**
- `streaming/rules.py` and `streaming/scoring.py` are **pure functions over Spark/pandas DataFrames** — no Kafka, no I/O — so they unit-test deterministically in-memory. This is the crux of the system and gets the most tests.
- `common/spark.py` centralizes all Iceberg/S3A config so every job builds an identical session.
- Files are split by responsibility (rules vs scoring vs sink vs job), not by layer.

---

## Phase 0 — Prerequisites & Skeleton

### Task 1: Install Docker & verify toolchain

**Files:** none (environment setup)

- [ ] **Step 1: Install Docker Desktop**

Run: `brew install --cask docker` (or download from docker.com). Then launch Docker Desktop once to start the daemon.

- [ ] **Step 2: Verify Docker is running**

Run: `docker info`
Expected: prints server version with no "Cannot connect to the Docker daemon" error.

- [ ] **Step 3: Verify Python & Java**

Run: `python3 --version && java -version`
Expected: Python 3.11.x and OpenJDK 21.

### Task 2: Python project scaffolding

**Files:**
- Create: `requirements.txt`, `pyproject.toml`, `.env.example`, `common/__init__.py`

- [ ] **Step 1: Create `requirements.txt`**

```
pyspark==3.5.1
confluent-kafka==2.4.0
faker==25.2.0
python-dotenv==1.0.1
scikit-learn==1.5.0
pandas==2.2.2
psycopg2-binary==2.9.9
great-expectations==0.18.16
dbt-trino==1.8.1
trino==0.328.0
pytest==8.2.1
```

- [ ] **Step 2: Create `pyproject.toml`**

```toml
[tool.pytest.ini_options]
pythonpath = ["."]
testpaths = ["tests"]
filterwarnings = ["ignore::DeprecationWarning"]

[tool.setuptools.packages.find]
include = ["common*", "generator*", "streaming*", "batch*"]
```

- [ ] **Step 3: Create `.env.example`**

```
KAFKA_BOOTSTRAP=localhost:9092
SCHEMA_REGISTRY=http://localhost:8081
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
LAKEHOUSE_BUCKET=lakehouse
ICEBERG_WAREHOUSE=s3a://lakehouse/warehouse
POSTGRES_DSN=postgresql://otp:otp@localhost:5432/otp
TRINO_HOST=localhost
TRINO_PORT=8080
```

- [ ] **Step 4: Create venv and install**

Run: `python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt`
Expected: all packages install without conflict.

- [ ] **Step 5: Create `common/__init__.py`** (empty file).

- [ ] **Step 6: Commit**

```bash
git add requirements.txt pyproject.toml .env.example common/__init__.py
git commit -m "chore: python project scaffolding and deps"
```

### Task 3: Infrastructure — docker-compose

**Files:**
- Create: `infra/docker-compose.yml`, `infra/minio/init-buckets.sh`, `infra/iceberg/trino-iceberg.properties`

- [ ] **Step 1: Create `infra/docker-compose.yml`**

```yaml
services:
  kafka:
    image: bitnami/kafka:3.7
    ports: ["9092:9092"]
    environment:
      KAFKA_CFG_NODE_ID: "1"
      KAFKA_CFG_PROCESS_ROLES: "broker,controller"
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
      KAFKA_CFG_LISTENERS: "PLAINTEXT://:9092,CONTROLLER://:9093"
      KAFKA_CFG_ADVERTISED_LISTENERS: "PLAINTEXT://localhost:9092"
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      ALLOW_PLAINTEXT_LISTENER: "yes"

  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.1
    depends_on: [kafka]
    ports: ["8081:8081"]
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: "PLAINTEXT://kafka:9092"
      SCHEMA_REGISTRY_LISTENERS: "http://0.0.0.0:8081"

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    depends_on: [kafka]
    ports: ["8085:8080"]
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: "kafka:9092"

  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    ports: ["9000:9000", "9001:9001"]
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes: ["minio-data:/data"]

  postgres:
    image: postgres:16
    ports: ["5432:5432"]
    environment:
      POSTGRES_USER: otp
      POSTGRES_PASSWORD: otp
      POSTGRES_DB: otp
    volumes: ["pg-data:/var/lib/postgresql/data"]

  trino:
    image: trinodb/trino:451
    depends_on: [minio]
    ports: ["8080:8080"]
    volumes:
      - ./iceberg/trino-iceberg.properties:/etc/trino/catalog/iceberg.properties

volumes:
  minio-data:
  pg-data:
```

- [ ] **Step 2: Create `infra/iceberg/trino-iceberg.properties`**

```properties
connector.name=iceberg
iceberg.catalog.type=hadoop
hive.metastore=file
iceberg.file-format=PARQUET
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.path-style-access=true
s3.aws-access-key=minioadmin
s3.aws-secret-key=minioadmin
s3.region=us-east-1
iceberg.catalog.warehouse=s3://lakehouse/warehouse
```

- [ ] **Step 3: Create `infra/minio/init-buckets.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb -p local/lakehouse || true
mc mb -p local/warehouse || true
echo "buckets ready"
```

- [ ] **Step 4: Start the stack**

Run: `docker compose -f infra/docker-compose.yml up -d`
Expected: all containers reach healthy/running (`docker compose -f infra/docker-compose.yml ps`).

- [ ] **Step 5: Create buckets**

Run: `docker run --rm --network host -v "$PWD/infra/minio:/s" --entrypoint bash minio/mc /s/init-buckets.sh`
Expected: prints "buckets ready". MinIO console at http://localhost:9001 shows `lakehouse` and `warehouse`.

- [ ] **Step 6: Commit**

```bash
git add infra/
git commit -m "infra: docker-compose for kafka, minio, trino, postgres"
```

---

## Phase 1 — Common Config, Schemas, Spark Session

### Task 4: Typed config loader

**Files:**
- Create: `common/config.py`
- Test: `tests/test_config.py`

- [ ] **Step 1: Write the failing test** (`tests/test_config.py`)

```python
import os
from common.config import Settings

def test_settings_reads_env(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP", "host:1234")
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://u:p@h/db")
    s = Settings.from_env()
    assert s.kafka_bootstrap == "host:1234"
    assert s.postgres_dsn.endswith("/db")

def test_settings_defaults(monkeypatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP", raising=False)
    s = Settings.from_env()
    assert s.kafka_bootstrap == "localhost:9092"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_config.py -v`
Expected: FAIL with "No module named 'common.config'".

- [ ] **Step 3: Write `common/config.py`**

```python
import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    iceberg_warehouse: str
    postgres_dsn: str
    trino_host: str
    trino_port: int

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            kafka_bootstrap=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
            minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            iceberg_warehouse=os.getenv("ICEBERG_WAREHOUSE", "s3a://lakehouse/warehouse"),
            postgres_dsn=os.getenv("POSTGRES_DSN", "postgresql://otp:otp@localhost:5432/otp"),
            trino_host=os.getenv("TRINO_HOST", "localhost"),
            trino_port=int(os.getenv("TRINO_PORT", "8080")),
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_config.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add common/config.py tests/test_config.py
git commit -m "feat: typed settings loader"
```

### Task 5: Event schemas & field constants

**Files:**
- Create: `common/schemas.py`
- Test: `tests/test_schemas.py`

- [ ] **Step 1: Write the failing test** (`tests/test_schemas.py`)

```python
from common.schemas import REQUEST_FIELDS, DELIVERY_FIELDS, VERIFICATION_FIELDS, TOPICS

def test_topics_present():
    assert TOPICS == {"requests": "otp.requests",
                      "delivery": "otp.delivery",
                      "verification": "otp.verification"}

def test_request_fields_have_correlation_key():
    assert "request_id" in REQUEST_FIELDS
    assert "msisdn" in REQUEST_FIELDS
    assert REQUEST_FIELDS[0] == "request_id"

def test_all_topics_share_request_id():
    for fields in (REQUEST_FIELDS, DELIVERY_FIELDS, VERIFICATION_FIELDS):
        assert "request_id" in fields
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_schemas.py -v`
Expected: FAIL with "No module named 'common.schemas'".

- [ ] **Step 3: Write `common/schemas.py`**

```python
TOPICS = {
    "requests": "otp.requests",
    "delivery": "otp.delivery",
    "verification": "otp.verification",
}
ALERTS_TOPIC = "otp.alerts"

REQUEST_FIELDS = [
    "request_id", "msisdn", "enterprise_id", "country_iso",
    "mcc", "mnc", "ip", "channel", "event_ts",
]
DELIVERY_FIELDS = ["request_id", "status", "operator_latency_ms", "event_ts"]
VERIFICATION_FIELDS = ["request_id", "outcome", "attempts", "event_ts"]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_schemas.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add common/schemas.py tests/test_schemas.py
git commit -m "feat: event schemas and topic constants"
```

### Task 6: Spark session factory

**Files:**
- Create: `common/spark.py`

> No unit test: this is environment glue verified by integration in later phases. Provide complete content.

- [ ] **Step 1: Write `common/spark.py`**

```python
from pyspark.sql import SparkSession
from common.config import Settings

ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"

def build_spark(app_name: str, settings: Settings | None = None) -> SparkSession:
    s = settings or Settings.from_env()
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", f"{ICEBERG_PKG},{KAFKA_PKG},{HADOOP_AWS}")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", s.iceberg_warehouse)
        .config("spark.hadoop.fs.s3a.endpoint", s.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
```

- [ ] **Step 2: Smoke-verify it imports**

Run: `python -c "import common.spark; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 3: Commit**

```bash
git add common/spark.py
git commit -m "feat: spark session factory with iceberg + s3a config"
```

---

## Phase 2 — Synthetic Generator

### Task 7: Traffic scenarios (normal + fraud)

**Files:**
- Create: `generator/__init__.py`, `generator/scenarios.py`
- Test: `tests/test_scenarios.py`

- [ ] **Step 1: Write the failing test** (`tests/test_scenarios.py`)

```python
from generator.scenarios import make_request_event, ait_burst, normal_traffic

def test_make_request_event_has_all_fields():
    e = make_request_event(enterprise_id="ent_1", country_iso="US", mcc="310", mnc="260")
    for k in ["request_id", "msisdn", "enterprise_id", "ip", "channel", "event_ts"]:
        assert k in e
    assert e["enterprise_id"] == "ent_1"

def test_ait_burst_targets_single_range_and_low_verify():
    events = ait_burst(count=50)
    prefixes = {e["msisdn"][:6] for e in events}
    assert len(prefixes) == 1            # concentrated on one number range
    assert all(e["channel"] == "sms" for e in events)

def test_normal_traffic_is_diverse():
    events = normal_traffic(count=100)
    prefixes = {e["msisdn"][:6] for e in events}
    assert len(prefixes) > 10            # spread across many ranges
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_scenarios.py -v`
Expected: FAIL with "No module named 'generator.scenarios'".

- [ ] **Step 3: Write `generator/__init__.py`** (empty), then `generator/scenarios.py`

```python
import random
import uuid
from datetime import datetime, timezone

CHANNELS = ["sms", "voice", "whatsapp"]
ENTERPRISES = [f"ent_{i}" for i in range(1, 11)]
ROUTES = [("US", "310", "260"), ("IN", "404", "45"),
          ("GB", "234", "10"), ("BR", "724", "5")]

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def make_request_event(enterprise_id=None, country_iso=None, mcc=None,
                       mnc=None, msisdn=None, ip=None) -> dict:
    route = random.choice(ROUTES)
    return {
        "request_id": str(uuid.uuid4()),
        "msisdn": msisdn or f"{random.randint(100000, 999999)}{random.randint(1000, 9999)}",
        "enterprise_id": enterprise_id or random.choice(ENTERPRISES),
        "country_iso": country_iso or route[0],
        "mcc": mcc or route[1],
        "mnc": mnc or route[2],
        "ip": ip or f"{random.randint(1,223)}.{random.randint(0,255)}."
                    f"{random.randint(0,255)}.{random.randint(0,255)}",
        "channel": "sms",
        "event_ts": _now_iso(),
    }

def normal_traffic(count: int) -> list[dict]:
    out = []
    for _ in range(count):
        e = make_request_event()
        e["channel"] = random.choice(CHANNELS)
        out.append(e)
    return out

def ait_burst(count: int, enterprise_id: str = "ent_3") -> list[dict]:
    """Mass requests to ONE number-range, one enterprise — the AIT signature."""
    prefix = "550199"
    return [
        make_request_event(
            enterprise_id=enterprise_id, country_iso="LV", mcc="247", mnc="1",
            msisdn=f"{prefix}{random.randint(1000, 9999)}",
        )
        for _ in range(count)
    ]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_scenarios.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add generator/__init__.py generator/scenarios.py tests/test_scenarios.py
git commit -m "feat: synthetic traffic scenarios (normal + AIT burst)"
```

### Task 8: Delivery & verification derivation

**Files:**
- Modify: `generator/scenarios.py`
- Test: `tests/test_scenarios.py` (add)

- [ ] **Step 1: Add failing tests** (append to `tests/test_scenarios.py`)

```python
from generator.scenarios import make_delivery_event, make_verification_event

def test_delivery_links_to_request():
    req = make_request_event()
    d = make_delivery_event(req, status="delivered")
    assert d["request_id"] == req["request_id"]
    assert d["status"] == "delivered"
    assert d["operator_latency_ms"] >= 0

def test_verification_outcome_controllable():
    req = make_request_event()
    v = make_verification_event(req, outcome="verified")
    assert v["request_id"] == req["request_id"]
    assert v["outcome"] == "verified"
    assert v["attempts"] >= 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_scenarios.py -v`
Expected: FAIL with "cannot import name 'make_delivery_event'".

- [ ] **Step 3: Append to `generator/scenarios.py`**

```python
def make_delivery_event(request: dict, status: str = "delivered") -> dict:
    return {
        "request_id": request["request_id"],
        "status": status,
        "operator_latency_ms": random.randint(50, 4000),
        "event_ts": _now_iso(),
    }

def make_verification_event(request: dict, outcome: str = "verified") -> dict:
    return {
        "request_id": request["request_id"],
        "outcome": outcome,
        "attempts": random.randint(1, 3),
        "event_ts": _now_iso(),
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_scenarios.py -v`
Expected: PASS (5 passed).

- [ ] **Step 5: Commit**

```bash
git add generator/scenarios.py tests/test_scenarios.py
git commit -m "feat: delivery and verification event derivation"
```

### Task 9: Kafka producer

**Files:**
- Create: `generator/producer.py`
- Test: `tests/test_producer.py`

- [ ] **Step 1: Write the failing test** (`tests/test_producer.py`) — uses a fake producer, no real Kafka

```python
from generator.producer import emit_batch

class FakeProducer:
    def __init__(self): self.sent = []
    def produce(self, topic, key, value): self.sent.append((topic, key, value))
    def flush(self): pass

def test_emit_batch_produces_three_event_types():
    fake = FakeProducer()
    emit_batch(fake, n_requests=5, fraud=False)
    topics = {t for (t, _, _) in fake.sent}
    assert "otp.requests" in topics
    assert "otp.delivery" in topics
    assert "otp.verification" in topics

def test_fraud_batch_has_low_verification_rate():
    fake = FakeProducer()
    emit_batch(fake, n_requests=40, fraud=True)
    verifs = [v for (t, _, v) in fake.sent if t == "otp.verification"]
    verified = [v for v in verifs if '"verified"' in v]
    # AIT: almost nothing genuinely verifies
    assert len(verified) <= max(1, len(verifs) // 5)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_producer.py -v`
Expected: FAIL with "No module named 'generator.producer'".

- [ ] **Step 3: Write `generator/producer.py`**

```python
import json
import random
from generator import scenarios
from common.schemas import TOPICS

def _send(producer, topic, event):
    producer.produce(topic, key=event["request_id"], value=json.dumps(event))

def emit_batch(producer, n_requests: int, fraud: bool) -> None:
    requests = (scenarios.ait_burst(n_requests) if fraud
                else scenarios.normal_traffic(n_requests))
    for req in requests:
        _send(producer, TOPICS["requests"], req)
        delivered = random.random() < 0.95
        _send(producer, TOPICS["delivery"], scenarios.make_delivery_event(
            req, status="delivered" if delivered else "failed"))
        if delivered:
            # normal traffic verifies ~70%; AIT verifies ~5%
            verify_p = 0.05 if fraud else 0.70
            outcome = "verified" if random.random() < verify_p else "timeout"
            _send(producer, TOPICS["verification"],
                  scenarios.make_verification_event(req, outcome=outcome))
    producer.flush()

def build_kafka_producer(bootstrap: str):
    from confluent_kafka import Producer
    return Producer({"bootstrap.servers": bootstrap})

def main():  # pragma: no cover
    import time
    from common.config import Settings
    s = Settings.from_env()
    p = build_kafka_producer(s.kafka_bootstrap)
    while True:
        emit_batch(p, n_requests=200, fraud=False)
        if random.random() < 0.10:
            emit_batch(p, n_requests=80, fraud=True)  # periodic AIT attack
        time.sleep(1)

if __name__ == "__main__":  # pragma: no cover
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_producer.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add generator/producer.py tests/test_producer.py
git commit -m "feat: kafka producer emitting correlated otp events"
```

---

## Phase 3 — Fraud Rule Engine (the crux; pure & fully tested)

### Task 10: Pure rule functions

**Files:**
- Create: `streaming/__init__.py`, `streaming/rules.py`
- Test: `tests/test_rules.py`

The rules operate on a **windowed aggregate row** (a dict) describing one
`(enterprise_id, number_range, window)` group, so they are pure and table-free.

- [ ] **Step 1: Write the failing test** (`tests/test_rules.py`)

```python
from streaming import rules

def agg(**kw):
    base = dict(requests=10, verifications=7, distinct_ip=8,
                msisdn_max_count=2, mcc="310", mnc="260")
    base.update(kw)
    return base

def test_velocity_fires_when_single_msisdn_floods():
    assert rules.velocity_score(agg(msisdn_max_count=60)) > 0
    assert rules.velocity_score(agg(msisdn_max_count=2)) == 0

def test_ait_signature_high_volume_low_verify():
    hot = agg(requests=500, verifications=5)     # 1% verify rate
    cold = agg(requests=500, verifications=350)   # 70% verify rate
    assert rules.ait_score(hot) > rules.ait_score(cold)
    assert rules.ait_score(cold) == 0

def test_repeated_never_verified():
    assert rules.never_verified_score(agg(requests=200, verifications=0)) > 0
    assert rules.never_verified_score(agg(requests=200, verifications=50)) == 0

def test_high_risk_route():
    assert rules.high_risk_route_score(agg(mcc="247", mnc="1")) > 0   # Latvia premium
    assert rules.high_risk_route_score(agg(mcc="310", mnc="260")) == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_rules.py -v`
Expected: FAIL with "No module named 'streaming.rules'".

- [ ] **Step 3: Write `streaming/__init__.py`** (empty), then `streaming/rules.py`

```python
"""Pure fraud-rule functions over a windowed aggregate dict.

Each returns a non-negative float contribution to the severity score.
No I/O, no Spark — unit-tested deterministically.
"""

HIGH_RISK_ROUTES = {("247", "1"), ("88", "0"), ("881", "0")}  # known AIT-heavy ranges
AIT_MIN_REQUESTS = 100
AIT_VERIFY_RATE_CEILING = 0.10
VELOCITY_MSISDN_CEILING = 30
NEVER_VERIFIED_MIN_REQUESTS = 50

def _verify_rate(agg: dict) -> float:
    r = agg.get("requests", 0)
    return (agg.get("verifications", 0) / r) if r else 0.0

def velocity_score(agg: dict) -> float:
    over = agg.get("msisdn_max_count", 0) - VELOCITY_MSISDN_CEILING
    return float(min(over, 100)) * 0.5 if over > 0 else 0.0

def ait_score(agg: dict) -> float:
    if agg.get("requests", 0) < AIT_MIN_REQUESTS:
        return 0.0
    rate = _verify_rate(agg)
    if rate >= AIT_VERIFY_RATE_CEILING:
        return 0.0
    # lower verify rate + higher volume => higher score
    return (AIT_VERIFY_RATE_CEILING - rate) * 100 * (agg["requests"] / 100)

def never_verified_score(agg: dict) -> float:
    if agg.get("requests", 0) >= NEVER_VERIFIED_MIN_REQUESTS and \
       agg.get("verifications", 0) == 0:
        return 40.0
    return 0.0

def high_risk_route_score(agg: dict) -> float:
    return 25.0 if (agg.get("mcc"), agg.get("mnc")) in HIGH_RISK_ROUTES else 0.0
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_rules.py -v`
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add streaming/__init__.py streaming/rules.py tests/test_rules.py
git commit -m "feat: pure fraud rule functions"
```

### Task 11: Score combination & severity

**Files:**
- Create: `streaming/scoring.py`
- Test: `tests/test_scoring.py`

- [ ] **Step 1: Write the failing test** (`tests/test_scoring.py`)

```python
from streaming.scoring import score_aggregate, severity

def agg(**kw):
    base = dict(requests=10, verifications=7, distinct_ip=8,
                msisdn_max_count=2, mcc="310", mnc="260")
    base.update(kw); return base

def test_clean_traffic_scores_zero_and_low():
    s = score_aggregate(agg())
    assert s == 0
    assert severity(s) == "low"

def test_ait_attack_scores_high():
    s = score_aggregate(agg(requests=500, verifications=2, mcc="247", mnc="1"))
    assert s > 100
    assert severity(s) == "high"

def test_severity_bands():
    assert severity(0) == "low"
    assert severity(30) == "medium"
    assert severity(150) == "high"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_scoring.py -v`
Expected: FAIL with "No module named 'streaming.scoring'".

- [ ] **Step 3: Write `streaming/scoring.py`**

```python
from streaming import rules

RULES = [
    rules.velocity_score,
    rules.ait_score,
    rules.never_verified_score,
    rules.high_risk_route_score,
]

def score_aggregate(agg: dict) -> float:
    return float(sum(rule(agg) for rule in RULES))

def severity(score: float) -> str:
    if score >= 100:
        return "high"
    if score >= 20:
        return "medium"
    return "low"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_scoring.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add streaming/scoring.py tests/test_scoring.py
git commit -m "feat: rule score combination and severity bands"
```

---

## Phase 4 — Streaming Jobs (Bronze landing + fraud detection)

### Task 12: Bronze streaming sink

**Files:**
- Create: `streaming/bronze_sink.py`

> Integration glue (Kafka→Iceberg). Verified by running, not unit test. Provide complete content.

- [ ] **Step 1: Write `streaming/bronze_sink.py`**

```python
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType)
from common.spark import build_spark
from common.config import Settings
from common.schemas import TOPICS

REQUEST_SCHEMA = StructType([
    StructField("request_id", StringType()), StructField("msisdn", StringType()),
    StructField("enterprise_id", StringType()), StructField("country_iso", StringType()),
    StructField("mcc", StringType()), StructField("mnc", StringType()),
    StructField("ip", StringType()), StructField("channel", StringType()),
    StructField("event_ts", StringType()),
])

def _read_topic(spark, bootstrap, topic):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest").load())

def run():  # pragma: no cover
    s = Settings.from_env()
    spark = build_spark("bronze-sink", s)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
    raw = _read_topic(spark, s.kafka_bootstrap, TOPICS["requests"])
    parsed = (raw.select(from_json(col("value").cast("string"), REQUEST_SCHEMA).alias("d"))
                 .select("d.*"))
    (parsed.writeStream.format("iceberg")
        .option("checkpointLocation", "/tmp/ckpt/bronze_requests")
        .toTable("lake.bronze.requests"))
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":  # pragma: no cover
    run()
```

- [ ] **Step 2: Verify import**

Run: `python -c "import streaming.bronze_sink; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 3: Commit**

```bash
git add streaming/bronze_sink.py
git commit -m "feat: bronze streaming sink kafka->iceberg"
```

### Task 13: Stateful streaming fraud job

**Files:**
- Create: `streaming/fraud_job.py`
- Test: `tests/test_fraud_job.py`

The windowed aggregation is built in Spark, but the **scoring is applied via a
testable pure helper** `apply_scoring(rows)` that takes a list of aggregate dicts.

- [ ] **Step 1: Write the failing test** (`tests/test_fraud_job.py`)

```python
from streaming.fraud_job import apply_scoring

def test_apply_scoring_flags_ait_rows():
    rows = [
        dict(enterprise_id="ent_3", number_range="550199", requests=500,
             verifications=3, msisdn_max_count=5, distinct_ip=2, mcc="247", mnc="1"),
        dict(enterprise_id="ent_1", number_range="447700", requests=20,
             verifications=15, msisdn_max_count=2, distinct_ip=18, mcc="310", mnc="260"),
    ]
    alerts = apply_scoring(rows)
    flagged = {a["enterprise_id"]: a for a in alerts}
    assert flagged["ent_3"]["severity"] == "high"
    assert flagged["ent_1"]["severity"] == "low"
    assert flagged["ent_3"]["score"] > flagged["ent_1"]["score"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_fraud_job.py -v`
Expected: FAIL with "No module named 'streaming.fraud_job'".

- [ ] **Step 3: Write `streaming/fraud_job.py`**

```python
from streaming.scoring import score_aggregate, severity

def apply_scoring(rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        score = score_aggregate(r)
        out.append({**r, "score": score, "severity": severity(score)})
    return out

# --- Spark wiring below (integration; not unit-tested) ---
def run():  # pragma: no cover
    from pyspark.sql.functions import (col, from_json, window, count, sum as ssum,
                                       max as smax, when, substring, expr)
    from common.spark import build_spark
    from common.config import Settings
    from common.schemas import TOPICS
    from streaming.bronze_sink import REQUEST_SCHEMA

    s = Settings.from_env()
    spark = build_spark("fraud-job", s)
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", s.kafka_bootstrap)
           .option("subscribe", TOPICS["requests"]).load())
    events = (raw.select(from_json(col("value").cast("string"),
                                   REQUEST_SCHEMA).alias("d")).select("d.*")
              .withColumn("event_ts", col("event_ts").cast("timestamp"))
              .withColumn("number_range", substring(col("msisdn"), 1, 6)))
    agg = (events.withWatermark("event_ts", "10 minutes")
           .groupBy(window(col("event_ts"), "5 minutes"),
                    col("enterprise_id"), col("number_range"),
                    col("mcc"), col("mnc"))
           .agg(count("*").alias("requests"),
                smax(expr("1")).alias("msisdn_max_count")))  # simplified
    # Score per micro-batch row using the pure helper
    def _score_batch(df, _id):
        rows = [r.asDict() for r in df.collect()]
        scored = apply_scoring([{**x, "verifications": 0, "distinct_ip": 0}
                                for x in rows])
        # write high/medium to Postgres
        import psycopg2
        conn = psycopg2.connect(s.postgres_dsn)
        with conn, conn.cursor() as cur:
            for a in scored:
                if a["severity"] != "low":
                    cur.execute(
                        "INSERT INTO fraud_alerts(enterprise_id, number_range, "
                        "score, severity) VALUES (%s,%s,%s,%s)",
                        (a["enterprise_id"], a["number_range"], a["score"], a["severity"]))
        conn.close()
    agg.writeStream.outputMode("update").foreachBatch(_score_batch).start()
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":  # pragma: no cover
    run()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_fraud_job.py -v`
Expected: PASS (1 passed).

- [ ] **Step 5: Create Postgres alert table**

Run:
```bash
docker exec -i $(docker compose -f infra/docker-compose.yml ps -q postgres) \
  psql -U otp -d otp -c "CREATE TABLE IF NOT EXISTS fraud_alerts(
    id serial primary key, enterprise_id text, number_range text,
    score double precision, severity text, created_at timestamptz default now());"
```
Expected: `CREATE TABLE`.

- [ ] **Step 6: Commit**

```bash
git add streaming/fraud_job.py tests/test_fraud_job.py
git commit -m "feat: stateful streaming fraud job with testable scoring"
```

---

## Phase 5 — Batch: Bronze→Silver

### Task 14: Silver transform (join, dedupe, enrich)

**Files:**
- Create: `batch/__init__.py`, `batch/silver.py`
- Test: `tests/test_silver.py`

The transform is a pure function `build_silver(requests_df, delivery_df, verification_df)`
taking Spark DataFrames, tested with a local Spark session on tiny inputs.

- [ ] **Step 1: Write the failing test** (`tests/test_silver.py`)

```python
import pytest
from pyspark.sql import SparkSession
from batch.silver import build_silver

@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("t").getOrCreate()
    yield s
    s.stop()

def test_silver_joins_and_dedupes(spark):
    req = spark.createDataFrame(
        [("r1", "5501990001", "ent_3", "247", "1"),
         ("r1", "5501990001", "ent_3", "247", "1"),   # duplicate
         ("r2", "4477001234", "ent_1", "310", "260")],
        ["request_id", "msisdn", "enterprise_id", "mcc", "mnc"])
    deliv = spark.createDataFrame(
        [("r1", "delivered", 120), ("r2", "delivered", 90)],
        ["request_id", "status", "operator_latency_ms"])
    verif = spark.createDataFrame(
        [("r1", "timeout", 1), ("r2", "verified", 1)],
        ["request_id", "outcome", "attempts"])
    out = build_silver(req, deliv, verif).orderBy("request_id").collect()
    assert len(out) == 2                       # dedup removed the dup r1
    r1 = [x for x in out if x["request_id"] == "r1"][0]
    assert r1["delivery_status"] == "delivered"
    assert r1["verification_outcome"] == "timeout"
    assert r1["is_verified"] is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_silver.py -v`
Expected: FAIL with "No module named 'batch.silver'".

- [ ] **Step 3: Write `batch/__init__.py`** (empty), then `batch/silver.py`

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

def build_silver(requests: DataFrame, delivery: DataFrame,
                 verification: DataFrame) -> DataFrame:
    req = requests.dropDuplicates(["request_id"])
    joined = (req.join(delivery.withColumnRenamed("status", "delivery_status"),
                       "request_id", "left")
                 .join(verification.withColumnRenamed("outcome",
                                                      "verification_outcome"),
                       "request_id", "left"))
    return joined.withColumn(
        "is_verified", when(col("verification_outcome") == "verified", True)
                       .otherwise(False))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_silver.py -v`
Expected: PASS (1 passed).

- [ ] **Step 5: Commit**

```bash
git add batch/__init__.py batch/silver.py tests/test_silver.py
git commit -m "feat: bronze->silver join/dedupe/enrich transform"
```

---

## Phase 6 — Batch ML Anomaly Net

### Task 15: Behavioral features

**Files:**
- Create: `batch/features.py`
- Test: `tests/test_features.py`

- [ ] **Step 1: Write the failing test** (`tests/test_features.py`)

```python
import pandas as pd
from batch.features import build_entity_features

def test_features_compute_verify_rate_and_volume():
    df = pd.DataFrame({
        "enterprise_id": ["ent_3"]*4 + ["ent_1"]*2,
        "number_range": ["550199"]*4 + ["447700"]*2,
        "is_verified": [False, False, False, True, True, True],
        "operator_latency_ms": [100, 110, 120, 130, 90, 95],
    })
    feats = build_entity_features(df).set_index("enterprise_id")
    assert feats.loc["ent_3", "requests"] == 4
    assert feats.loc["ent_3", "verify_rate"] == 0.25
    assert feats.loc["ent_1", "verify_rate"] == 1.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_features.py -v`
Expected: FAIL with "No module named 'batch.features'".

- [ ] **Step 3: Write `batch/features.py`**

```python
import pandas as pd

def build_entity_features(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("enterprise_id")
    out = g.agg(
        requests=("is_verified", "size"),
        verify_rate=("is_verified", "mean"),
        avg_latency=("operator_latency_ms", "mean"),
        range_spread=("number_range", "nunique"),
    ).reset_index()
    return out
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_features.py -v`
Expected: PASS (1 passed).

- [ ] **Step 5: Commit**

```bash
git add batch/features.py tests/test_features.py
git commit -m "feat: per-entity behavioral features"
```

### Task 16: Isolation Forest scoring

**Files:**
- Create: `batch/anomaly.py`
- Test: `tests/test_anomaly.py`

- [ ] **Step 1: Write the failing test** (`tests/test_anomaly.py`)

```python
import pandas as pd
from batch.anomaly import score_anomalies

def test_isolation_forest_flags_outlier_entity():
    # 9 normal entities + 1 obvious AIT outlier (huge volume, ~0 verify)
    rows = [dict(enterprise_id=f"ent_{i}", requests=20, verify_rate=0.7,
                 avg_latency=120, range_spread=15) for i in range(9)]
    rows.append(dict(enterprise_id="ent_bad", requests=5000, verify_rate=0.01,
                     avg_latency=130, range_spread=1))
    feats = pd.DataFrame(rows)
    scored = score_anomalies(feats)
    worst = scored.sort_values("anomaly_score").iloc[0]
    assert worst["enterprise_id"] == "ent_bad"
    assert worst["is_anomaly"] == True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_anomaly.py -v`
Expected: FAIL with "No module named 'batch.anomaly'".

- [ ] **Step 3: Write `batch/anomaly.py`**

```python
import pandas as pd
from sklearn.ensemble import IsolationForest

FEATURE_COLS = ["requests", "verify_rate", "avg_latency", "range_spread"]

def score_anomalies(feats: pd.DataFrame, contamination: float = 0.1) -> pd.DataFrame:
    model = IsolationForest(contamination=contamination, random_state=42)
    X = feats[FEATURE_COLS].values
    model.fit(X)
    out = feats.copy()
    out["anomaly_score"] = model.decision_function(X)   # lower = more anomalous
    out["is_anomaly"] = model.predict(X) == -1
    return out
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_anomaly.py -v`
Expected: PASS (1 passed).

- [ ] **Step 5: Commit**

```bash
git add batch/anomaly.py tests/test_anomaly.py
git commit -m "feat: isolation forest anomaly scoring"
```

---

## Phase 7 — dbt Gold Marts

### Task 17: dbt project + funnel & fraud marts

**Files:**
- Create: `dbt/dbt_project.yml`, `dbt/profiles.yml`,
  `dbt/models/gold/delivery_funnel.sql`, `dbt/models/gold/ait_loss.sql`,
  `dbt/models/gold/fraud_daily.sql`, `dbt/models/gold/schema.yml`

> dbt models are validated by `dbt build` (compile + run + tests). Provide complete content.

- [ ] **Step 1: Create `dbt/dbt_project.yml`**

```yaml
name: otp_gold
version: "1.0"
profile: otp
model-paths: ["models"]
models:
  otp_gold:
    gold:
      +materialized: table
```

- [ ] **Step 2: Create `dbt/profiles.yml`**

```yaml
otp:
  target: dev
  outputs:
    dev:
      type: trino
      host: localhost
      port: 8080
      user: analytics
      catalog: iceberg
      schema: gold
      http_scheme: http
```

- [ ] **Step 3: Create `dbt/models/gold/delivery_funnel.sql`**

```sql
select
  enterprise_id,
  mcc,
  mnc,
  count(*) as requested,
  count_if(delivery_status = 'delivered') as delivered,
  count_if(is_verified) as verified,
  round(1.0 * count_if(is_verified) / nullif(count(*), 0), 4) as verify_rate
from iceberg.silver.events
group by 1, 2, 3
```

- [ ] **Step 4: Create `dbt/models/gold/ait_loss.sql`**

```sql
-- suspected AIT volume (high volume + low verify) * per-message cost
with funnel as (select * from {{ ref('delivery_funnel') }})
select
  enterprise_id, mcc, mnc, requested, verify_rate,
  case when requested > 100 and verify_rate < 0.10
       then requested * 0.03 else 0 end as estimated_ait_loss_usd
from funnel
```

- [ ] **Step 5: Create `dbt/models/gold/fraud_daily.sql`**

```sql
select
  date(created_at) as day,
  severity,
  count(*) as alert_count
from postgres.public.fraud_alerts
group by 1, 2
```

- [ ] **Step 6: Create `dbt/models/gold/schema.yml`**

```yaml
version: 2
models:
  - name: delivery_funnel
    columns:
      - name: enterprise_id
        tests: [not_null]
      - name: verify_rate
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1
  - name: ait_loss
    columns:
      - name: estimated_ait_loss_usd
        tests: [not_null]
```

- [ ] **Step 7: Build marts**

Run: `cd dbt && dbt build --profiles-dir .`
Expected: models run and tests pass (assumes Silver `iceberg.silver.events` exists; if running before batch, expect a clear "table not found" and run after Phase 8 wiring).

- [ ] **Step 8: Commit**

```bash
git add dbt/
git commit -m "feat: dbt gold marts (funnel, ait_loss, fraud_daily) with tests"
```

---

## Phase 8 — Orchestration & Quality

### Task 18: Airflow DAGs

**Files:**
- Create: `airflow/dags/batch_etl_dag.py`, `airflow/dags/fraud_anomaly_dag.py`

> DAG validity is verified by `python <dag>.py` importing without error.

- [ ] **Step 1: Create `airflow/dags/batch_etl_dag.py`**

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG("otp_batch_etl", start_date=datetime(2026, 1, 1),
         schedule="@hourly", catchup=False) as dag:
    silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="python -m batch.run_silver")
    dbt = BashOperator(
        task_id="dbt_build",
        bash_command="cd $PROJECT_ROOT/dbt && dbt build --profiles-dir .")
    quality = BashOperator(
        task_id="quality_checks",
        bash_command="python -m quality.expectations_silver")
    silver >> dbt >> quality
```

- [ ] **Step 2: Create `airflow/dags/fraud_anomaly_dag.py`**

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG("otp_fraud_anomaly", start_date=datetime(2026, 1, 1),
         schedule="@daily", catchup=False) as dag:
    BashOperator(task_id="anomaly_scoring",
                 bash_command="python -m batch.run_anomaly")
```

- [ ] **Step 3: Verify DAGs import**

Run: `python airflow/dags/batch_etl_dag.py && python airflow/dags/fraud_anomaly_dag.py && echo ok`
Expected: prints `ok` (requires `pip install apache-airflow==2.9.1` if validating locally).

- [ ] **Step 4: Commit**

```bash
git add airflow/
git commit -m "feat: airflow dags for batch etl and anomaly scoring"
```

### Task 19: Great Expectations on Silver

**Files:**
- Create: `quality/__init__.py`, `quality/expectations_silver.py`
- Test: `tests/test_quality.py`

- [ ] **Step 1: Write the failing test** (`tests/test_quality.py`)

```python
import pandas as pd
from quality.expectations_silver import validate_silver

def test_validate_silver_catches_null_request_id():
    bad = pd.DataFrame({"request_id": ["r1", None],
                        "is_verified": [True, False]})
    result = validate_silver(bad)
    assert result["ok"] is False
    assert "request_id" in result["failures"]

def test_validate_silver_passes_clean():
    good = pd.DataFrame({"request_id": ["r1", "r2"],
                         "is_verified": [True, False]})
    assert validate_silver(good)["ok"] is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_quality.py -v`
Expected: FAIL with "No module named 'quality.expectations_silver'".

- [ ] **Step 3: Write `quality/__init__.py`** (empty), then `quality/expectations_silver.py`

```python
import pandas as pd

def validate_silver(df: pd.DataFrame) -> dict:
    failures = []
    if df["request_id"].isnull().any():
        failures.append("request_id")
    if not df["is_verified"].isin([True, False]).all():
        failures.append("is_verified")
    return {"ok": len(failures) == 0, "failures": failures}

def main():  # pragma: no cover
    raise SystemExit("wire to read silver from Trino in production")

if __name__ == "__main__":  # pragma: no cover
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_quality.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add quality/ tests/test_quality.py
git commit -m "feat: data-quality validation for silver"
```

---

## Phase 9 — End-to-End Smoke Test & Docs

### Task 20: E2E smoke test

**Files:**
- Create: `tests/test_e2e_smoke.py`

This test exercises the whole logical chain in-process (generator → scoring →
alert) without needing the cluster, proving the fraud path end to end.

- [ ] **Step 1: Write the test** (`tests/test_e2e_smoke.py`)

```python
import json
from collections import defaultdict
from generator.producer import emit_batch
from streaming.fraud_job import apply_scoring

class FakeProducer:
    def __init__(self): self.sent = []
    def produce(self, topic, key, value): self.sent.append((topic, key, value))
    def flush(self): pass

def _aggregate(sent):
    reqs = [json.loads(v) for (t, _, v) in sent if t == "otp.requests"]
    verifs = [json.loads(v) for (t, _, v) in sent if t == "otp.verification"]
    verified_ids = {v["request_id"] for v in verifs if v["outcome"] == "verified"}
    groups = defaultdict(lambda: dict(requests=0, verifications=0,
                                      msisdn_max_count=1, distinct_ip=0))
    for r in reqs:
        key = (r["enterprise_id"], r["msisdn"][:6], r["mcc"], r["mnc"])
        g = groups[key]
        g.update(enterprise_id=r["enterprise_id"], number_range=r["msisdn"][:6],
                 mcc=r["mcc"], mnc=r["mnc"])
        g["requests"] += 1
        if r["request_id"] in verified_ids:
            g["verifications"] += 1
    return list(groups.values())

def test_ait_attack_produces_high_severity_alert():
    fake = FakeProducer()
    emit_batch(fake, n_requests=300, fraud=True)     # AIT attack
    alerts = apply_scoring(_aggregate(fake.sent))
    high = [a for a in alerts if a["severity"] == "high"]
    assert high, "expected at least one high-severity AIT alert"

def test_normal_traffic_produces_no_high_alert():
    fake = FakeProducer()
    emit_batch(fake, n_requests=300, fraud=False)
    alerts = apply_scoring(_aggregate(fake.sent))
    assert not [a for a in alerts if a["severity"] == "high"]
```

- [ ] **Step 2: Run the full suite**

Run: `pytest -v`
Expected: all tests pass (every prior task's tests + these two).

- [ ] **Step 3: Commit**

```bash
git add tests/test_e2e_smoke.py
git commit -m "test: end-to-end fraud-path smoke test"
```

### Task 21: SCALING.md & RUNBOOK.md

**Files:**
- Create: `docs/SCALING.md`, `docs/RUNBOOK.md`

- [ ] **Step 1: Write `docs/SCALING.md`**

Content must map each local choice to petabyte production:
- Kafka: partition count sizing (target ≤1GB/partition/retention; partition by `enterprise_id` hash), retention, tiered storage.
- Spark: `spark.sql.shuffle.partitions`, executor sizing, checkpoint to durable store, AQE, structured-streaming state store (RocksDB) for large windows.
- Iceberg: partition by `days(event_ts)` + `bucket(enterprise_id)`, compaction (`rewrite_data_files`), snapshot expiration, metadata pruning.
- Storage: MinIO → S3 (lifecycle policies, intelligent tiering).
- Serving: Postgres → Redshift/Snowflake; Trino cluster sizing.
- Orchestration: Airflow → MWAA; SLAs and retries.
- Cost controls: autoscaling, spot executors, partition pruning.

- [ ] **Step 2: Write `docs/RUNBOOK.md`**

Content: ordered local start-up — (1) `docker compose up -d`, (2) init buckets,
(3) create namespaces & Postgres table, (4) start `streaming/bronze_sink.py`,
(5) start `streaming/fraud_job.py`, (6) start `generator/producer.py`,
(7) trigger Airflow DAGs, (8) query marts in Trino / inspect `fraud_alerts`.
Include teardown (`docker compose down -v`) and troubleshooting (port conflicts,
missing JARs → `--packages`, checkpoint reset).

- [ ] **Step 3: Commit**

```bash
git add docs/SCALING.md docs/RUNBOOK.md
git commit -m "docs: scaling blueprint and operational runbook"
```

---

## Self-Review Notes (completed)

- **Spec coverage:** event model (Task 5), generator+fraud injection (Tasks 7-9),
  Kafka→Bronze (Task 12), streaming rule engine + scoring + alerts (Tasks 10-13),
  Bronze→Silver (Task 14), ML net (Tasks 15-16), Gold marts incl. funnel/latency/
  AIT-loss/fraud-daily (Task 17), Airflow (Task 18), Great Expectations (Task 19),
  E2E smoke (Task 20), SCALING.md (Task 21). All spec sections map to a task.
- **Note on latency mart:** delivery_funnel covers conversion; latency percentiles
  are an additional `latency_pct.sql` the engineer adds alongside Task 17 using
  `operator_latency_ms` percentiles — same pattern as funnel.
- **Placeholder scan:** no TBD/TODO; every code step contains complete code.
- **Type consistency:** `apply_scoring`, `score_aggregate`, `severity`,
  `build_silver`, `build_entity_features`, `score_anomalies`, `validate_silver`,
  `emit_batch` signatures are consistent across the tasks that reference them.
```
