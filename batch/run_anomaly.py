"""Batch entrypoint: daily Isolation-Forest anomaly scoring over Silver.

Wires the unit-tested ``build_entity_features`` and ``score_anomalies`` to the
real lakehouse and writes anomalies to Postgres. Run by the Airflow
``otp_fraud_anomaly`` DAG. Integration glue — not unit tested.
"""
import psycopg2
from common.spark import build_spark
from common.config import Settings
from batch.features import build_entity_features
from batch.anomaly import score_anomalies


def run():  # pragma: no cover
    s = Settings.from_env()
    spark = build_spark("batch-anomaly", s)
    pdf = (spark.table("lake.silver.events")
           .select("enterprise_id", "number_range", "is_verified",
                   "operator_latency_ms")
           .toPandas())
    feats = build_entity_features(pdf)
    scored = score_anomalies(feats)
    anomalies = scored[scored["is_anomaly"]]

    conn = psycopg2.connect(s.postgres_dsn)
    with conn, conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS fraud_anomalies(
            enterprise_id text, requests bigint, verify_rate double precision,
            anomaly_score double precision, scored_at timestamptz default now())""")
        for _, r in anomalies.iterrows():
            cur.execute(
                "INSERT INTO fraud_anomalies(enterprise_id, requests, verify_rate, "
                "anomaly_score) VALUES (%s,%s,%s,%s)",
                (r["enterprise_id"], int(r["requests"]),
                 float(r["verify_rate"]), float(r["anomaly_score"])))
    conn.close()
    spark.stop()


if __name__ == "__main__":  # pragma: no cover
    run()
