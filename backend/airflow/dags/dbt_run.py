from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import subprocess
import os

default_args = {
    "owner": "mobi-roger",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 1, 1),
}

dag = DAG(
    dag_id="dbt_run",
    default_args=default_args,
    schedule="@hourly",
    catchup=False,
    description="Transforms raw_bike_trips into analytics-ready marts via dbt",
)

DBT_PROJECT_DIR = os.path.join(os.path.dirname(__file__), "../../dbt")
DBT_PROFILES_DIR = os.path.join(os.path.dirname(__file__), "../../dbt")

# Runs a dbt command and streams output to Airflow logs
def run_dbt_command(command: list[str]):
    """
    Executes a dbt CLI command inside the dbt project directory.
    Raises an error if the command exits with a non-zero code.
    """
    full_command = [
        "dbt", *command,
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROFILES_DIR,
    ]

    print(f"Running: {' '.join(full_command)}")

    result = subprocess.run(
        full_command,
        capture_output=True,
        text=True,
    )

    # Stream stdout and stderr to Airflow logs
    print(result.stdout)
    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"dbt command failed: {' '.join(command)}")


# Wait for bike_trips_etl to finish successfully
wait_for_etl = ExternalTaskSensor(
    task_id="wait_for_etl",
    external_dag_id="bike_trips_etl",
    external_task_id="load",          
    allowed_states=["success"],
    failed_states=["failed", "skipped"],
    timeout=3600,                      
    poke_interval=60,                  
    mode="poke",
    dag=dag,
)


# Installs any dbt packages defined in packages.yml
def dbt_deps(**context):
    run_dbt_command(["deps"])

# Runs only the staging layer first to clean raw data
def dbt_run_staging(**context):
    run_dbt_command(["run", "--select", "staging"])

# Builds intermediate models on top of staging
def dbt_run_intermediate(**context):
    run_dbt_command(["run", "--select", "intermediate"])

# Builds final analytics-ready tables that FastAPI will query
def dbt_run_marts(**context):
    run_dbt_command(["run", "--select", "marts"])


t1_deps = PythonOperator(
    task_id="dbt_deps",
    python_callable=dbt_deps,
    dag=dag,
)

t2_staging = PythonOperator(
    task_id="dbt_run_staging",
    python_callable=dbt_run_staging,
    dag=dag,
)

t3_intermediate = PythonOperator(
    task_id="dbt_run_intermediate",
    python_callable=dbt_run_intermediate,
    dag=dag,
)

t4_marts = PythonOperator(
    task_id="dbt_run_marts",
    python_callable=dbt_run_marts,
    dag=dag,
)
