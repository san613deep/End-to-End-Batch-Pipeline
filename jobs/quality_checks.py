"""Inspects the data for known quality problems, including duplicate trip IDs, missing values, invalid distances, and negative fares.

Important distinction: a quality report identifies problems; it does not necessarily repair them. The transformation job is responsible for producing the cleaned output."""


from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    project_root = Path(__file__).resolve().parents[1]
    input_path = project_root / "data" / "processed" / "raw_parquet"

    spark = SparkSession.builder.appName("TaxiDataQualityChecks").getOrCreate()

    try:
        df = spark.read.parquet(str(input_path))

        # Total number of records
        total_rows = df.count()

        # Find duplicate trip IDs.
        duplicate_id_groups = df.groupBy("trip_id").count().filter(F.col("count") > 1)

        duplicate_id_groups.show(truncate=False)

        duplicate_extra_rows = sum(
            row["count"] - 1 for row in duplicate_id_groups.collect()
        )

        # Count individual quality issues.
        missing_trip_ids = df.filter(F.col("trip_id").isNull()).count()

        missing_passengers = df.filter(F.col("passenger_count").isNull()).count()

        zero_or_negative_distance = df.filter(F.col("trip_distance") <= 0).count()

        negative_fares = df.filter(F.col("fare_amount") < 0).count()

        missing_pickup_times = df.filter(F.col("pickup_datetime").isNull()).count()

        missing_dropoff_times = df.filter(F.col("dropoff_datetime").isNull()).count()

        # Print a readable report.
        report = [
            ("Total records", total_rows),
            ("Extra rows from duplicate trip IDs", duplicate_extra_rows),
            ("Rows with missing trip IDs", missing_trip_ids),
            ("Rows with missing passenger counts", missing_passengers),
            ("Rows with zero or negative distance", zero_or_negative_distance),
            ("Rows with negative fares", negative_fares),
            ("Rows with missing pickup times", missing_pickup_times),
            ("Rows with missing dropoff times", missing_dropoff_times),
        ]

        print("\n========== DATA QUALITY REPORT ==========")

        for check_name, issue_count in report:
            print(f"{check_name}: {issue_count}")

        print("==========================================\n")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
