"""Performs the transformation and writes the cleaned Parquet dataset. The output you verified contained four trip IDs: 1, 2, 4, and 7.

Concepts to understand: filtering, deduplication, data types, DataFrame transformations, and writing output. Check the actual code to see the precise rule applied to each record."""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def main():
    project_root = Path(__file__).resolve().parents[1]

    input_path = project_root / "data" / "processed" / "raw_parquet"
    output_path = project_root / "data" / "processed" / "cleaned_parquet"

    spark = SparkSession.builder.appName("TaxiDataTransformation").getOrCreate()

    try:
        # 1. Read the raw Parquet data.
        df = spark.read.parquet(str(input_path))
        input_count = df.count()

        print(f"Input records: {input_count}")

        # 2. Remove duplicate trip IDs.
        # Keep the earliest-ingested record for each ID.
        window = Window.partitionBy("trip_id").orderBy(
            F.col("ingestion_timestamp").asc(),
            F.col("source_file").asc(),
        )

        df = (
            df.withColumn("_row_number", F.row_number().over(window))
            .filter(F.col("_row_number") == 1)
            .drop("_row_number")
        )

        # 3. Remove records with missing required values,
        # invalid distances/fares, or invalid timestamps.
        df = df.filter(
            F.col("trip_id").isNotNull()
            & F.col("passenger_count").isNotNull()
            & (F.col("passenger_count") > 0)
            & F.col("pickup_datetime").isNotNull()
            & F.col("dropoff_datetime").isNotNull()
            & (F.col("dropoff_datetime") > F.col("pickup_datetime"))
            & F.col("trip_distance").isNotNull()
            & (F.col("trip_distance") > 0)
            & F.col("fare_amount").isNotNull()
            & (F.col("fare_amount") >= 0)
        )

        # 4. Calculate trip duration in minutes.
        df = df.withColumn(
            "trip_duration_minutes",
            (
                F.col("dropoff_datetime").cast("long")
                - F.col("pickup_datetime").cast("long")
            )
            / 60.0,
        )

        # 5. Save cleaned data separately.
        output_count = df.count()

        (df.write.mode("overwrite").parquet(str(output_path)))

        print("\n========== TRANSFORMATION REPORT ==========")
        print(f"Input records: {input_count}")
        print(f"Clean records: {output_count}")
        print(f"Records excluded: {input_count - output_count}")
        print(f"Output location: {output_path}")
        print("============================================")

        print("\nCleaned data preview:")
        df.orderBy("trip_id").show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
