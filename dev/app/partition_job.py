from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder.appName("ignis-partition-test")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Shuffle 500k rows across only 2 partitions.
# ignis should fire: 2 < 2 * num_cores → under-partitioned WARNING.
df = spark.range(500_000).select(
    (col("id") % 100).alias("key"),
    col("id").alias("value"),
)
df.groupBy("key").count().write.mode("overwrite").parquet("/tmp/ignis_partition_test")
print("write complete")

spark.stop()
