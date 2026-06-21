"""Batch entrypoint: build the Silver table from Bronze Iceberg tables.

Wires the unit-tested ``build_silver`` transform to the real lakehouse.
Run by the Airflow ``otp_batch_etl`` DAG. Integration glue — not unit tested.
"""
from common.spark import build_spark
from batch.silver import build_silver


def run():  # pragma: no cover
    spark = build_spark("batch-silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    requests = spark.table("lake.bronze.requests")
    delivery = spark.table("lake.bronze.delivery")
    verification = spark.table("lake.bronze.verification")
    silver = build_silver(requests, delivery, verification)
    (silver.writeTo("lake.silver.events").createOrReplace())
    spark.stop()


if __name__ == "__main__":  # pragma: no cover
    run()
