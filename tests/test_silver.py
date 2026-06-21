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
