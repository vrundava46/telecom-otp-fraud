from streaming.scoring import score_aggregate, severity


def apply_scoring(rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        score = score_aggregate(r)
        out.append({**r, "score": score, "severity": severity(score)})
    return out


# --- Spark wiring below (integration; not unit-tested) ---
def run():  # pragma: no cover
    from pyspark.sql.functions import (col, from_json, window, count,
                                       max as smax, substring, expr)
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

    def _score_batch(df, _id):
        rows = [r.asDict() for r in df.collect()]
        scored = apply_scoring([{**x, "verifications": 0, "distinct_ip": 0}
                                for x in rows])
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
