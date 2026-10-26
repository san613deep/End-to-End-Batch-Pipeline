from pathlib import Path
from pyspark.sql import SparkSession

project_root = Path(__file__).resolve().parents[1]
input_path = project_root / "data" / "processed" / "cleaned_parquet"

spark = SparkSession.builder.appName("VerifyCleanedData").getOrCreate()

try:
    df = spark.read.parquet(str(input_path))

    print("\n========== CLEANED DATA VERIFICATION ==========")
    print(f"Cleaned rows: {df.count()}")
    print("Trip IDs expected: 1, 2, 4, 7")
    print("Actual trip IDs:")

    df.select("trip_id").orderBy("trip_id").show()

    print("Trip durations:")
    df.select("trip_id", "trip_duration_minutes").orderBy("trip_id").show()

    print("================================================")
finally:
    spark.stop()
