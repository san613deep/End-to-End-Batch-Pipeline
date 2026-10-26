import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("BatchPipelineSetupTest")
    .master("local[*]")
    .getOrCreate()
)

data = [
    (1, "Delhi", 250.50),
    (2, "Mumbai", 400.00),
    (3, "Delhi", 180.00),
]

df = spark.createDataFrame(
    data,
    ["trip_id", "city", "fare"]
)

df.show()

print("Total rows:", df.count())

spark.stop()
