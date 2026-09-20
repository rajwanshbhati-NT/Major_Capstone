from datetime import datetime
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator


def _postgres_conn():
    return psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
    )


def log_failure_details(**context):
    """Log failure details from ingestion_count table."""
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}

    execution_id = conf.get("execution_id")
    if not execution_id:
        print("[FailureHandler] No execution_id in conf")
        return

    conn = _postgres_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT execution_id, file_path, file_name, file_type,
                       target_table, expected_count, actual_count, difference,
                       status, failure_reason, execution_timestamp, creation_timestamp
                FROM ingestion_count
                WHERE execution_id = %s
                """,
                (execution_id,),
            )
            row = cur.fetchone()

            if row:
                print("=" * 60)
                print("[FAILURE HANDLER] Ingestion Failure Details")
                print("=" * 60)
                print(f"  Execution ID     : {row[0]}")
                print(f"  File Path        : {row[1]}")
                print(f"  File Name        : {row[2]}")
                print(f"  File Type        : {row[3]}")
                print(f"  Target Table     : {row[4]}")
                print(f"  Expected Count   : {row[5]}")
                print(f"  Actual Count     : {row[6]}")
                print(f"  Difference       : {row[7]}")
                print(f"  Status           : {row[8]}")
                print(f"  Failure Reason   : {row[9]}")
                print(f"  Execution Time   : {row[10]}")
                print(f"  Logged At        : {row[11]}")
                print("=" * 60)
            else:
                print(f"[FailureHandler] No record found for execution_id={execution_id}")
    finally:
        conn.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "retries": 0,
}

with DAG(
    dag_id="sor_ingestion_failure_handler",
    default_args=default_args,
    description="Handles failures from sor_ingestion_phase1",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "failure", "phase1"],
) as dag:

    log_failure = PythonOperator(
        task_id="log_failure",
        python_callable=log_failure_details,
        provide_context=True,
    )
