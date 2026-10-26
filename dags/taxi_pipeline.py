import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_IMAGE = "apache/spark:4.2.0"
PROJECT_HOST_PATH = os.environ.get("PROJECT_HOST_PATH", "")


def spark_command(script_name: str) -> str:
    if not PROJECT_HOST_PATH:
        raise ValueError("PROJECT_HOST_PATH is not configured for Airflow")

    return (
        "docker run --rm "
        f'-v "{PROJECT_HOST_PATH}/jobs:/opt/project/jobs" '
        f'-v "{PROJECT_HOST_PATH}/data:/opt/project/data" '
        "-w /opt/project "
        f"{SPARK_IMAGE} "
        f"/opt/spark/bin/spark-submit jobs/{script_name}"
    )


with DAG(
    dag_id="taxi_data_pipeline",
    description="Orchestrate the taxi Spark batch pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["taxi", "data-engineering"],
) as dag:
    raw_ingestion = BashOperator(
        task_id="raw_ingestion",
        bash_command=spark_command("raw_ingest.py"),
    )

    data_quality_checks = BashOperator(
        task_id="data_quality_checks",
        bash_command=spark_command("quality_checks.py"),
    )

    data_transformation = BashOperator(
        task_id="data_transformation",
        bash_command=spark_command("transform.py"),
    )

    verify_cleaned_output = BashOperator(
        task_id="verify_cleaned_output",
        bash_command=spark_command("verify_cleaned.py"),
    )

    raw_ingestion >> data_quality_checks >> data_transformation >> verify_cleaned_output
