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
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
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
