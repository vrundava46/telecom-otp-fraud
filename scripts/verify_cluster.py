"""Bounded live end-to-end verification against the real Docker cluster.

Proves the infra-dependent path: Kafka -> Spark -> Iceberg-on-MinIO -> Postgres,
plus Trino visibility. Uses trigger=availableNow so the streaming step terminates.

Run (with cluster up): PYTHONPATH=. python scripts/verify_cluster.py
"""
import json
import sys

import psycopg2
from pyspark.sql.functions import col, from_json, count, substring

from common.spark import build_spark
from common.config import Settings
from common.schemas import TOPICS
from streaming.bronze_sink import REQUEST_SCHEMA
from streaming.fraud_job import apply_scoring
from batch.silver import build_silver
from generator import scenarios


def produce_events(bootstrap: str) -> int:
    from confluent_kafka import Producer
    p = Producer({"bootstrap.servers": bootstrap})
    reqs = scenarios.normal_traffic(60) + scenarios.ait_burst(200)
    for r in reqs:
        p.produce(TOPICS["requests"], key=r["request_id"], value=json.dumps(r))
    p.flush()
    return len(reqs)


def main() -> int:
    s = Settings.from_env()

    n = produce_events(s.kafka_bootstrap)
    print(f"[1/5] Produced {n} events to Kafka topic {TOPICS['requests']}")

    spark = build_spark("verify-cluster", s)
    spark.sparkContext.setLogLevel("ERROR")

    # [2] Spark BATCH read Kafka -> Bronze Iceberg on MinIO
    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", s.kafka_bootstrap)
           .option("subscribe", TOPICS["requests"])
           .option("startingOffsets", "earliest").load())
    reqs = (raw.select(from_json(col("value").cast("string"),
                                 REQUEST_SCHEMA).alias("d")).select("d.*"))
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
    reqs.writeTo("lake.bronze.requests").createOrReplace()
    bronze = spark.table("lake.bronze.requests").count()
    assert bronze == n, f"expected {n} bronze rows, got {bronze}"
    print(f"[2/5] Kafka -> Bronze Iceberg (MinIO) round-trip OK ({bronze} rows)")

    # [3] Streaming (availableNow) -> score -> Postgres alerts
    conn = psycopg2.connect(s.postgres_dsn)
    with conn, conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS fraud_alerts(
            id serial primary key, enterprise_id text, number_range text,
            score double precision, severity text,
            created_at timestamptz default now())""")
        cur.execute("TRUNCATE fraud_alerts")
    conn.close()

    def _score_batch(df, _id):
        grouped = (df.select(from_json(col("value").cast("string"),
                                       REQUEST_SCHEMA).alias("d")).select("d.*")
                     .withColumn("number_range", substring(col("msisdn"), 1, 6))
                     .groupBy("enterprise_id", "number_range", "mcc", "mnc")
                     .agg(count("*").alias("requests")))
        rows = [{**r.asDict(), "verifications": 0} for r in grouped.collect()]
        scored = apply_scoring(rows)
        c = psycopg2.connect(s.postgres_dsn)
        with c, c.cursor() as cur:
            for a in scored:
                if a["severity"] != "low":
                    cur.execute(
                        "INSERT INTO fraud_alerts(enterprise_id, number_range, "
                        "score, severity) VALUES (%s,%s,%s,%s)",
                        (a["enterprise_id"], a["number_range"], a["score"],
                         a["severity"]))
        c.close()

    q = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", s.kafka_bootstrap)
         .option("subscribe", TOPICS["requests"])
         .option("startingOffsets", "earliest").load()
         .writeStream.foreachBatch(_score_batch).trigger(availableNow=True).start())
    q.awaitTermination()

    conn = psycopg2.connect(s.postgres_dsn)
    with conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM fraud_alerts WHERE severity='high'")
        highs = cur.fetchone()[0]
    conn.close()
    assert highs >= 1, f"expected >=1 high-severity alert, got {highs}"
    print(f"[3/5] Streaming Kafka -> scoring -> Postgres OK ({highs} high alert(s))")

    # [4] Bronze -> Silver Iceberg
    # build delivery/verification bronze too so the join has all three inputs
    deliv = spark.createDataFrame(
        [(r["request_id"], "delivered", 100) for r in
         [x.asDict() for x in spark.table("lake.bronze.requests").collect()]],
        ["request_id", "status", "operator_latency_ms"])
    verif = spark.createDataFrame(
        [(r["request_id"], "timeout", 1) for r in
         [x.asDict() for x in spark.table("lake.bronze.requests").collect()]],
        ["request_id", "outcome", "attempts"])
    silver = build_silver(spark.table("lake.bronze.requests"), deliv, verif)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    silver.writeTo("lake.silver.events").createOrReplace()
    sc = spark.table("lake.silver.events").count()
    assert sc == n, f"expected {n} silver rows, got {sc}"
    print(f"[4/5] Bronze -> Silver Iceberg OK ({sc} rows)")

    # [5] Trino visibility of the same Iceberg tables
    import trino
    tc = trino.dbapi.connect(host=s.trino_host, port=s.trino_port, user="verify",
                             catalog="iceberg")
    cur = tc.cursor()
    cur.execute("SELECT count(*) FROM iceberg.silver.events")
    trino_count = cur.fetchone()[0]
    assert trino_count == n, f"Trino saw {trino_count}, expected {n}"
    print(f"[5/5] Trino reads Iceberg silver.events OK ({trino_count} rows)")

    spark.stop()
    print("\nALL LIVE CLUSTER CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
