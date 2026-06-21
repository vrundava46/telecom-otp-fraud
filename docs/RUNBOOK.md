# Runbook — Telecom OTP-Fraud Pipeline

## Prerequisites
- Docker Desktop installed and running (`docker info` succeeds).
- Python venv with deps: `python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt`

## Start-up (ordered)
1. **Bring up infrastructure**
   ```bash
   docker compose -f infra/docker-compose.yml up -d
   docker compose -f infra/docker-compose.yml ps   # wait for all healthy
   ```
2. **Create lakehouse buckets** (attach the `mc` client to the compose network —
   macOS Docker Desktop has no usable host networking)
   ```bash
   NET=$(docker network ls --format '{{.Name}}' | grep -i infra | head -1)
   docker run --rm --network "$NET" --entrypoint sh minio/mc -c \
     "mc alias set local http://minio:9000 minioadmin minioadmin && \
      mc mb -p local/lakehouse local/warehouse"
   ```
3. **Create the Postgres alert table**
   ```bash
   docker exec -i "$(docker compose -f infra/docker-compose.yml ps -q postgres)" \
     psql -U otp -d otp -c "CREATE TABLE IF NOT EXISTS fraud_alerts(
       id serial primary key, enterprise_id text, number_range text,
       score double precision, severity text, created_at timestamptz default now());"
   ```
4. **Start the Bronze landing stream** (terminal 1)
   ```bash
   python -m streaming.bronze_sink
   ```
5. **Start the real-time fraud job** (terminal 2)
   ```bash
   python -m streaming.fraud_job
   ```
6. **Start the event generator** (terminal 3)
   ```bash
   python -m generator.producer
   ```
7. **Run batch ETL + marts** — trigger the Airflow DAGs `otp_batch_etl` and
   `otp_fraud_anomaly`, or run their steps directly:
   ```bash
   python -m batch.run_silver
   (cd dbt && dbt build --profiles-dir .)
   python -m batch.run_anomaly
   ```
8. **Inspect results**
   - Fraud alerts: `psql -U otp -d otp -c "SELECT severity, count(*) FROM fraud_alerts GROUP BY 1;"`
   - Marts in Trino: `SELECT * FROM iceberg.gold.ait_loss ORDER BY estimated_ait_loss_usd DESC;`
   - Kafka UI: http://localhost:8085 · MinIO console: http://localhost:9001 · Trino: http://localhost:8080

## Verification scripts
- `PYTHONPATH=. python scripts/verify_iceberg_local.py` — Docker-free check of the
  Spark + Iceberg + batch + streaming logic (local-filesystem catalog).
- `PYTHONPATH=. python scripts/verify_cluster.py` — bounded live end-to-end check
  against the running stack: Kafka → Spark → Iceberg-on-MinIO → Postgres → Trino.
  Requires the stack up and buckets created (steps 1–2).

## Teardown
```bash
docker compose -f infra/docker-compose.yml down -v   # -v also drops volumes
rm -rf /tmp/ckpt                                      # clear streaming checkpoints
```

## Troubleshooting
- **Port already in use:** another service holds 9092/9000/5432/8080 — stop it or remap
  ports in `infra/docker-compose.yml`.
- **Spark `ClassNotFound` / missing connectors:** the JAR coordinates in
  `common/spark.py` (`spark.jars.packages`) are downloaded on first run; ensure network
  access, or pre-stage the ivy cache.
- **Stream not advancing / replays:** delete the relevant checkpoint under `/tmp/ckpt`
  to reset offsets, then restart the job.
- **Trino can't see Iceberg tables:** confirm `infra/iceberg/trino-iceberg.properties`
  is mounted and the `lakehouse` bucket exists.
