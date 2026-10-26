# End-to-End Batch Data Pipeline

An end-to-end batch data engineering project that ingests raw taxi trip data, stores it in Parquet format, performs data quality checks, transforms the data, and orchestrates the workflow using Apache Airflow and Apache Spark.

## Architecture

```text
Raw Taxi CSV
     |
     v
Raw Ingestion (Apache Spark)
     |
     v
Raw Parquet Data
     |
     v
Data Quality Checks
     |
     v
Data Transformation
     |
     v
Cleaned Parquet Data
     |
     v
Output Verification
```

Apache Airflow orchestrates the four pipeline tasks. Spark jobs execute in Docker containers, while Airflow manages task dependencies and execution status.

## Technologies Used

- Python
- Apache Spark 4.2.0
- Apache Airflow 2.10.5
- Docker and Docker Compose
- PostgreSQL 16
- Parquet
- Pytest

## Project Structure

```text
End-to-End-Batch-Pipeline/
├── data/
│   ├── raw/
│   │   └── taxi_trips.csv
│   └── processed/
│       ├── raw_parquet/
│       └── cleaned_parquet/
├── dags/
│   └── taxi_pipeline.py
├── jobs/
│   ├── raw_ingest.py
│   ├── quality_checks.py
│   ├── transform.py
│   ├── verify_cleaned.py
│   ├── verify_parquet.py
│   └── spark_test.py
├── tests/
│   └── test_pipeline.py
├── Dockerfile.test
├── docker-compose.yaml
├── requirements.txt
└── README.md
```

## Pipeline Tasks

1. **Raw ingestion:** Reads the taxi CSV dataset and writes raw Parquet output.
2. **Data quality checks:** Checks duplicate trip IDs, missing values, invalid distances, and negative fares.
3. **Data transformation:** Produces cleaned Parquet output.
4. **Output verification:** Reads the cleaned output and verifies the resulting records and trip durations.

## Data Quality Checks

The sample dataset contains eight records and intentionally includes data quality issues to test the pipeline.

Checks include:

- Duplicate trip IDs
- Missing passenger counts
- Zero or negative trip distances
- Negative fares
- Missing pickup or drop-off timestamps

## Run the Project

Start the Airflow services from the project directory:

```powershell
docker compose up -d
```

Open the Airflow web interface:

`http://localhost:8080`

Log in using your locally configured Airflow credentials. Find `taxi_data_pipeline` and trigger a DAG run.

The pipeline executes the four tasks in dependency order. Check the task status and logs in the Airflow UI.

## Run Automated Tests

The project includes a Docker-based Pytest test suite. Build the test image and run the tests using the commands configured for this project:

```powershell
docker build -f Dockerfile.test -t taxi-spark-test:4.2.0 .
docker run --rm -v "${PWD}\jobs:/opt/project/jobs" -v "${PWD}\data:/opt/project/data" taxi-spark-test:4.2.0
```

## Output

- Raw data: `data/processed/raw_parquet/`
- Cleaned data: `data/processed/cleaned_parquet/`

Spark writes Parquet part files and a `_SUCCESS` marker when output writing completes successfully.

## Current Status

- [x] Raw CSV ingestion
- [x] Parquet storage
- [x] Data quality checks
- [x] Data transformation
- [x] Output verification
- [x] Airflow orchestration
- [x] Docker-based Spark execution
- [x] Automated test passed

## Future Improvements

- Add a larger, more realistic dataset
- Add configurable data quality thresholds
- Introduce structured logging and failure alerts
- Add incremental processing and partitioned Parquet output
- Add cloud storage integration
- Add CI/CD testing through GitHub Actions
