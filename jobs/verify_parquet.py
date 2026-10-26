
from pathlib import Path
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("VerifyParquet")
    .getOrCreate()
)

project_root = Path(__file__).resolve().parents[1]
parquet_path = project_root / "data" / "processed" / "raw_parquet"

df = spark.read.parquet(str(parquet_path))

print("\n--- Parquet schema ---")
df.printSchema()

print("\n--- Parquet data ---")
df.show(truncate=False)

print(f"Total rows in Parquet: {df.count()}")

spark.stop()
