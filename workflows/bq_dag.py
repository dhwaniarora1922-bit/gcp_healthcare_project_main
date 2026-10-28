
# import all modules
import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)

# Define constants
PROJECT_ID = "healthcare-rcm-project-504309"
LOCATION = "US"

# Composer's mounted GCS data directory
SQL_BASE_PATH = "/home/airflow/gcs/data/BQ"

SQL_FILE_PATH_1 = f"{SQL_BASE_PATH}/bronze.sql"

SILVER_SQL_FILES = [
    ("silver_patients", f"{SQL_BASE_PATH}/silver_patients.sql"),
    ("silver_transactions", f"{SQL_BASE_PATH}/silver_transactions.sql"),
    ("silver_encounters", f"{SQL_BASE_PATH}/silver_encounters.sql"),
    ("silver_claims", f"{SQL_BASE_PATH}/silver_claims.sql"),
    ("silver_departments", f"{SQL_BASE_PATH}/silver_departments.sql"),
    ("silver_providers", f"{SQL_BASE_PATH}/silver_providers.sql"),
    ("silver_cptcodes", f"{SQL_BASE_PATH}/silver_cptcodes.sql"),
]

GOLD_SQL_FILES = [
    ("gold_patient_history", f"{SQL_BASE_PATH}/gold_patient_history.sql"),
    ("gold_provider_performance_summary", f"{SQL_BASE_PATH}/gold_provider_performance_summary.sql"),
    ("gold_department_performance_analytics", f"{SQL_BASE_PATH}/gold_department_performance_analytics.sql"),
    ("gold_financial_metrics", f"{SQL_BASE_PATH}/gold_financial_metrics.sql"),
    (
        "gold_payor_performance_claims_summary",
        f"{SQL_BASE_PATH}/gold_payor_performance_&_claims_summary.sql",
    ),
    ("gold_provider_charge_summary", f"{SQL_BASE_PATH}/gold_provider_charge_summary.sql"),
]

# Read SQL query from file
def read_sql_file(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()


# Read Bronze SQL
BRONZE_QUERY = read_sql_file(SQL_FILE_PATH_1)

# Define default arguments
ARGS = {
    "owner": "healthcare_rcm",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email_on_success": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="bigquery_dag",
    schedule_interval=None,
    catchup=False,
    description="DAG to run Healthcare RCM BigQuery jobs",
    default_args=ARGS,
    tags=["gcs", "bigquery", "etl", "healthcare-rcm"],
) as dag:

    # Task to create/update Bronze tables
    bronze_tables = BigQueryInsertJobOperator(
        task_id="bronze_tables",
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={
            "query": {
                "query": BRONZE_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    # Create individual Silver tasks
    silver_tasks = []

    for task_name, sql_file_path in SILVER_SQL_FILES:
        sql_query = read_sql_file(sql_file_path)

        silver_task = BigQueryInsertJobOperator(
            task_id=task_name,
            project_id=PROJECT_ID,
            location=LOCATION,
            configuration={
                "query": {
                    "query": sql_query,
                    "useLegacySql": False,
                    "priority": "BATCH",
                }
            },
        )

        silver_tasks.append(silver_task)

    # Create individual Gold tasks
    gold_tasks = []

    for task_name, sql_file_path in GOLD_SQL_FILES:
        sql_query = read_sql_file(sql_file_path)

        gold_task = BigQueryInsertJobOperator(
            task_id=task_name,
            project_id=PROJECT_ID,
            location=LOCATION,
            configuration={
                "query": sql_query,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        )

        gold_tasks.append(gold_task)

# Define dependencies:
# Bronze -> Silver SQL tasks -> Gold SQL tasks
bronze_tables >> silver_tasks

for silver_task in silver_tasks:
    silver_task >> gold_tasks


