# Scaling Blueprint — Local → Petabyte Production

This pipeline runs locally on docker-compose, but every component was chosen to map
cleanly onto petabyte-scale production. This document is the bridge.

## Kafka (event backbone)
- **Local:** single broker, default partitions.
- **Production:** partition each topic by `hash(enterprise_id)` so a single tenant's
  traffic spreads across partitions. Size partitions to keep ≤ ~1 GB of retained data
  per partition per retention window; start at 50–200 partitions for `otp.requests`
  and scale with peak TPS (target ≤ 10 MB/s per partition). Enable tiered storage so
  cold segments offload to object storage. Replication factor 3, `min.insync.replicas=2`.

## Spark (stream + batch)
- **Local:** `local[*]`, micro-batch.
- **Production:** run on YARN/Kubernetes. Tune `spark.sql.shuffle.partitions` to
  ~2–3× total executor cores; enable Adaptive Query Execution. For streaming, use the
  **RocksDB state store** for large windowed state and checkpoint to S3 (not local
  disk). Right-size executors (memory per core ~4–8 GB) and use dynamic allocation.
  Partition pruning + predicate pushdown on Iceberg keeps scans bounded.

## Iceberg (lakehouse tables)
- **Local:** hadoop catalog on MinIO.
- **Production:** REST or Glue catalog. Partition Bronze/Silver by
  `days(event_ts)` and `bucket(N, enterprise_id)`. Schedule maintenance:
  `rewrite_data_files` (compaction) to fight small-file growth from streaming,
  `expire_snapshots` and `remove_orphan_files` for retention/cost. Iceberg metadata
  pruning + hidden partitioning avoids full-table scans at petabyte scale.

## Object storage
- **Local:** MinIO (S3 API).
- **Production:** S3 with lifecycle policies (Bronze → IA/Glacier after N days),
  intelligent tiering, and bucket-level encryption. The S3A configuration in
  `common/spark.py` is identical in shape — only endpoint/credentials change.

## Serving
- **Local:** Postgres (alerts/marts) + Trino (lakehouse queries).
- **Production:** Postgres → Redshift/Snowflake/BigQuery for the serving warehouse;
  Trino as a multi-worker cluster (separate coordinator + autoscaling workers) for
  interactive lakehouse SQL. dbt models are unchanged across these targets.

## Orchestration
- **Local:** Airflow via docker-compose / CLI.
- **Production:** managed Airflow (MWAA / Cloud Composer). Add SLAs, retries with
  exponential backoff, and data-freshness sensors gating downstream marts.

## Fraud detection at scale
- Rules run **in-stream** (constant per-event cost) so detection latency stays flat as
  volume grows — the speed layer scales horizontally with Kafka partitions and Spark
  executors. The Isolation-Forest net runs **offline/batch** so model cost never sits
  on the hot path. Promote stable rules into a feature store + online model only if
  real-time ML is later justified.

## Cost controls
- Spot/preemptible executors for batch; autoscaling Trino workers; aggressive
  partition pruning; compaction to reduce S3 request counts; tiered Kafka + S3 storage.
