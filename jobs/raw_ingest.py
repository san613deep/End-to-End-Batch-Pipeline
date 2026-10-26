import os
import sys
from datetime import datetime, timezone
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_FILE = PROJECT_ROOT / "data" / "raw" / "taxi_trips.csv"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "raw_parquet"

spark = (
    SparkSession.builder.appName("TaxiRawIngestion").master("local[*]").getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Dataset not found: {INPUT_FILE}")

    ingestion_time = datetime.now(timezone.utc).isoformat()

    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(INPUT_FILE))
    )

    df = df.withColumn("ingestion_timestamp", lit(ingestion_time)).withColumn(
        "source_file", lit(INPUT_FILE.name)
    )

    df.write.mode("overwrite").parquet(str(OUTPUT_DIR))

    print(f"Source file: {INPUT_FILE.name}")
    print(f"Rows ingested: {df.count()}")
    print(f"Raw Parquet saved to: {OUTPUT_DIR}")

finally:
    spark.stop()
