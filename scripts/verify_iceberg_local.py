"""Docker-free integration check: exercise the real Spark + Iceberg data path.

Uses a LOCAL-filesystem Iceberg hadoop catalog (no MinIO/S3) to prove that:
  1. Spark can create Iceberg namespaces/tables and round-trip data.
  2. ``build_silver`` runs on Iceberg-sourced DataFrames and lands a Silver table.
  3. The batch feature + Isolation-Forest path runs on the Silver extract.
  4. Spark Structured Streaming applies ``apply_scoring`` in foreachBatch.

Run: python scripts/verify_iceberg_local.py
"""
import sys
import tempfile

from pyspark.sql import SparkSession

from generator import scenarios
from batch.silver import build_silver
from batch.features import build_entity_features
from batch.anomaly import score_anomalies
from streaming.fraud_job import apply_scoring

ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"


def build_local_iceberg_spark(warehouse: str) -> SparkSession:
    return (
        SparkSession.builder.appName("verify-iceberg-local")
        .config("spark.jars.packages", ICEBERG_PKG)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", f"file://{warehouse}")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def main() -> int:
    warehouse = tempfile.mkdtemp(prefix="iceberg_verify_")
    spark = build_local_iceberg_spark(warehouse)
    spark.sparkContext.setLogLevel("ERROR")

    # --- 1. Build a small dataset: normal traffic + one AIT burst ---
    reqs = scenarios.normal_traffic(60) + scenarios.ait_burst(200)
    deliv = [scenarios.make_delivery_event(r, "delivered") for r in reqs]
    # AIT burst (mcc 247) verifies rarely; normal verifies often
    verif = [scenarios.make_verification_event(
                 r, "verified" if r["mcc"] != "247" else "timeout") for r in reqs]

    req_df = spark.createDataFrame(reqs).select(
        "request_id", "msisdn", "enterprise_id", "mcc", "mnc")
    deliv_df = spark.createDataFrame(deliv).select(
        "request_id", "status", "operator_latency_ms")
    verif_df = spark.createDataFrame(verif).select(
        "request_id", "outcome", "attempts")

    # --- 2. Round-trip Bronze through Iceberg ---
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
    req_df.writeTo("lake.bronze.requests").createOrReplace()
    deliv_df.writeTo("lake.bronze.delivery").createOrReplace()
    verif_df.writeTo("lake.bronze.verification").createOrReplace()
    bronze_count = spark.table("lake.bronze.requests").count()
    assert bronze_count == 260, f"expected 260 bronze rows, got {bronze_count}"
    print(f"[1/4] Iceberg Bronze round-trip OK ({bronze_count} rows)")

    # --- 3. Silver transform on Iceberg-sourced DataFrames ---
    silver = build_silver(spark.table("lake.bronze.requests"),
                          spark.table("lake.bronze.delivery"),
                          spark.table("lake.bronze.verification"))
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    silver.writeTo("lake.silver.events").createOrReplace()
    s = spark.table("lake.silver.events")
    cols = set(s.columns)
    assert {"number_range", "is_verified", "delivery_status"} <= cols, cols
    assert s.count() == 260
    print(f"[2/4] Silver Iceberg table OK ({s.count()} rows, cols include "
          "number_range/is_verified)")

    # --- 4. Batch feature + anomaly path on the Silver extract ---
    pdf = s.select("enterprise_id", "number_range", "is_verified",
                   "operator_latency_ms").toPandas()
    feats = build_entity_features(pdf)
    scored = score_anomalies(feats)
    n_anom = int(scored["is_anomaly"].sum())
    print(f"[3/4] Feature + IsolationForest path OK "
          f"({len(feats)} entities, {n_anom} flagged anomalous)")

    # --- 5. Structured Streaming groups events and applies scoring (file source) ---
    from pyspark.sql.functions import col, count, substring

    stream_dir = tempfile.mkdtemp(prefix="otp_stream_")
    ait_df = spark.createDataFrame(scenarios.ait_burst(200)).select(
        "request_id", "msisdn", "enterprise_id", "mcc", "mnc")
    ait_df.write.mode("overwrite").json(stream_dir)   # pre-stage events as files

    captured: list[dict] = []

    def _score_batch(df, _id):
        grouped = (df.withColumn("number_range", substring(col("msisdn"), 1, 6))
                     .groupBy("enterprise_id", "number_range", "mcc", "mnc")
                     .agg(count("*").alias("requests")))
        rows = [{**r.asDict(), "verifications": 0} for r in grouped.collect()]
        captured.extend(apply_scoring(rows))

    q = (spark.readStream.schema(ait_df.schema).json(stream_dir)
         .writeStream.foreachBatch(_score_batch).start())
    q.processAllAvailable()
    q.stop()
    assert any(a["severity"] == "high" for a in captured), captured
    print("[4/4] Structured Streaming + scoring OK "
          "(streamed AIT burst grouped -> high severity)")

    spark.stop()
    print("\nALL ICEBERG/SPARK INTEGRATION CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
