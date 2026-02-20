from airflow.sdk import dag, task
from datetime import datetime, timedelta
import subprocess
import os

DBT_PROJECT_DIR = os.path.join(os.path.dirname(__file__), "../dbt")
DBT_PROFILES_DIR = os.path.join(os.path.dirname(__file__), "../dbt")


def run_dbt_command(command: list[str]):
    # Executes a dbt CLI command inside the dbt project directory.
    full_command = [
        "dbt",*command,
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROFILES_DIR,
        "--log-level-file", "none"
    ]

    print(f"Running: {' '.join(full_command)}")

    result = subprocess.run(
        full_command,
        capture_output=True,
        text=True,
    )

    # Stream stdout and stderr to Airflow logs
    # print(result)
    print(result.stdout)
    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"dbt command failed: {' '.join(command)}")


@dag(
    dag_id="dbt_run",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    description="Transforms raw_bike_trips into analytics-ready marts via dbt",
    default_args={
        "owner": "mobi-roger",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["dbt", "analytics"],
)
def dbt_run_dag():

    @task()
    def dbt_deps():
        # Installs any dbt packages defined in packages.yml
        run_dbt_command(["deps"])

    @task()
    def dbt_run_staging():
        # Runs only the staging layer first to clean raw data
        print("Staging")
        run_dbt_command(["run", "--select", "staging"])
        print("Created view successfully")

    @task()
    def dbt_run_intermediate():
        # Builds intermediate models on top of staging
        run_dbt_command(["run", "--select", "intermediate"])

    @task()
    def dbt_run_marts():
        # Builds final analytics-ready tables that FastAPI will query
        run_dbt_command(["run", "--select", "marts"])

    # dbt_deps() >>
    dbt_run_staging() >> dbt_run_intermediate()
    #  >> dbt_run_marts()


# Instantiate the DAG
dbt_run_dag()