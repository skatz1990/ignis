from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ignis-skew-test").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 95% of records carry key=0 — one post-shuffle task handles ~85x more data than any other.
n = 500_000
data = [(0 if i < int(n * 0.95) else (i % 9) + 1, i) for i in range(n)]
df = spark.createDataFrame(data, ["key", "value"])

df.groupBy("key").count().show()

spark.stop()
