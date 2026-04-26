from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2

spark = (
    SparkSession.builder.appName("ignis-spill-test")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")
    # Shrink execution memory to force spill.
    .config("spark.memory.fraction", "0.3")
    .config("spark.memory.storageFraction", "0.3")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Sort 3M rows of incompressible SHA-256 data (~600 MB) with only 30% of
# heap available for execution. Spark must spill to disk.
n = 3_000_000
df = spark.range(n).select(
    (col("id") % 10).alias("key"),
    sha2(col("id").cast("string"), 256).alias("h1"),
    sha2((col("id") * 31).cast("string"), 256).alias("h2"),
    sha2((col("id") * 37).cast("string"), 256).alias("h3"),
)
df.sort("h1", "h2").write.mode("overwrite").parquet("/tmp/ignis_spill_test")
print("write complete")

spark.stop()
