from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CLEANED_PATH = PROJECT_ROOT / "data" / "processed" / "cleaned_parquet"


def test_cleaned_pipeline_output():
    """Verify that the transformation job produced valid cleaned data."""
    assert CLEANED_PATH.exists(), (
        f"Cleaned dataset not found: {CLEANED_PATH}. Run jobs/transform.py first."
    )

    spark = (
        SparkSession.builder.appName("TaxiPipelineTests")
        .master("local[1]")
        .getOrCreate()
    )

    try:
        df = spark.read.parquet(str(CLEANED_PATH))

        # The sample dataset should produce four clean records.
        assert df.count() == 4

        # The expected trip IDs should be present.
        actual_ids = {row["trip_id"] for row in df.select("trip_id").collect()}
        assert actual_ids == {1, 2, 4, 7}

        # Required values must not be missing.
        assert (
            df.filter(
                F.col("trip_id").isNull()
                | F.col("passenger_count").isNull()
                | F.col("pickup_datetime").isNull()
                | F.col("dropoff_datetime").isNull()
            ).count()
            == 0
        )

        # Distances must be positive and fares non-negative.
        assert (
            df.filter(
                (F.col("trip_distance") <= 0) | (F.col("fare_amount") < 0)
            ).count()
            == 0
        )

        # Every dropoff must occur after pickup.
        assert (
            df.filter(F.col("dropoff_datetime") <= F.col("pickup_datetime")).count()
            == 0
        )

        # Check calculated trip durations.
        durations = {
            row["trip_id"]: row["trip_duration_minutes"]
            for row in df.select("trip_id", "trip_duration_minutes").collect()
        }

        assert durations == {
            1: 15.0,
            2: 20.0,
            4: 35.0,
            7: 30.0,
        }

    finally:
        spark.stop()
