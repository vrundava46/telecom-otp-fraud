from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
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
