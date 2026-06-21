from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, substring


def build_silver(requests: DataFrame, delivery: DataFrame,
                 verification: DataFrame) -> DataFrame:
    req = requests.dropDuplicates(["request_id"])
    joined = (req.join(delivery.withColumnRenamed("status", "delivery_status"),
                       "request_id", "left")
                 .join(verification.withColumnRenamed("outcome",
                                                      "verification_outcome"),
                       "request_id", "left"))
    return (joined
            .withColumn("number_range", substring(col("msisdn"), 1, 6))
            .withColumn("is_verified",
                        when(col("verification_outcome") == "verified", True)
                        .otherwise(False)))
