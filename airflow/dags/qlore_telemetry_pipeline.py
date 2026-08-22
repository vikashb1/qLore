from datetime import timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime


DEFAULT_ARGS = {
    "owner": "qlore",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="qlore_telemetry_pipeline",
    description="qLore Bronze to Silver to Gold telemetry pipeline",
    default_args=DEFAULT_ARGS,

    # Fixed historical start date allows scheduled catchup/backfill.
    start_date=datetime(
        2026,
        8,
        1,
        tz="UTC",
    ),

    schedule="@hourly",

    catchup=False,

    # Prevent overlapping transformation runs.
    max_active_runs=1,

    tags=[
        "qlore",
        "telemetry",
        "iceberg",
        "data-engineering",
    ],
) as dag:

    check_bronze_quality = BashOperator(
        task_id="check_bronze_quality",

        bash_command=(
            "cd /opt/airflow/qlore && "
            "python -m quality.run_bronze_quality"
        ),

        execution_timeout=timedelta(
            minutes=5
        ),
    )

    transform_silver = BashOperator(
        task_id="transform_silver",

        bash_command=(
            "cd /opt/airflow/qlore && "
            "python transformations/silver/telemetry_transform.py"
        ),

        execution_timeout=timedelta(
            minutes=10
        ),
    )

    transform_gold = BashOperator(
        task_id="transform_gold",

        bash_command=(
            "cd /opt/airflow/qlore && "
            "python transformations/gold/device_health_transform.py"
        ),

        execution_timeout=timedelta(
            minutes=10
        ),
    )

    pipeline_complete = BashOperator(
        task_id="pipeline_complete",

        bash_command=(
            'echo "qLore telemetry pipeline completed successfully."'
        ),
    )

    check_bronze_quality >> transform_silver >> transform_gold >> pipeline_complete