from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, when

spark = (
    SparkSession.builder.appName("ignis-skew-test")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.shuffle.partitions", "10")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# repartition() shuffles raw records (no partial aggregation) so key=0's
# ~4.75M rows land in one partition. sha2 makes duration scale with record
# count — parquet file-creation has a fixed ~900ms cost that would flatten
# durations on smaller data sizes, masking the skew from DataSkewRule.
# spark.range avoids building a large Python list in the driver.
n = 8_000_000
df = spark.range(n).select(
    when(col("id") < int(n * 0.95), 0).otherwise((col("id") % 9) + 1).cast("int").alias("key"),
    col("id").alias("value"),
)

df.repartition(10, "key") \
    .withColumn("hash", sha2(col("value").cast("string"), 256)) \
    .write.mode("overwrite").parquet("/tmp/ignis_skew_result")

spark.stop()
