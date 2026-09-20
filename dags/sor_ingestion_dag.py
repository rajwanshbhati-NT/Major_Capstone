from __future__ import annotations
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException, AirflowSkipException


DEFAULT_ARGS = {
    "owner": "rajwansh",
    "depends_on_past": False,
    "retries": 0,
    "email_on_failure": False,
}

BEAM_JAR = os.environ.get("BEAM_JAR", "/opt/airflow/beam-java/target/beam-ingestion.jar")
ERRORS_DIR = os.environ.get("ERRORS_DIR", "/opt/airflow/data/output/errors")
WAREHOUSE_JDBC_URL = os.environ.get("WAREHOUSE_JDBC_URL", "jdbc:postgresql://warehouse-db:5432/warehouse")
WAREHOUSE_USERNAME = os.environ.get("WAREHOUSE_USERNAME", "warehouse")
WAREHOUSE_PASSWORD = os.environ.get("WAREHOUSE_PASSWORD", "warehouse")



def _get_postgres_conn(database="airflow"):
    return psycopg2.connect(
        host="warehouse-db", database=database,
        user="warehouse", password="warehouse",
    )


def _normalize_path(path: str) -> str:
    if not path:
        return path
    if path.startswith("/opt/"):
        return path
    if path.startswith("C:/") or path.startswith("c:/"):
        return path.replace("C:/", "/opt/airflow/data/").replace("c:/", "/opt/airflow/data/")
    if path.startswith("C:\\") or path.startswith("c:\\"):
        return path.replace("C:\\", "/opt/airflow/data/").replace("c:\\", "/opt/airflow/data/")
    return path


def _insert_running_record(**context):
    conf = context["dag_run"].conf or {}
    execution_id = conf.get("execution_id")
    source_file_paths = conf.get("source_file_paths", [conf.get("source_file_path", "")])
    file_path = source_file_paths[0] if source_file_paths else ""
    file_name = os.path.basename(file_path) if file_path else None
    file_type = conf.get("file_format")
    target_table = conf.get("target_table")

    if not execution_id:
        raise AirflowFailException("execution_id missing in conf")

    conn = _get_postgres_conn(database="warehouse")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingestion_count
                    (execution_id, file_path, file_name, file_type, target_table,
                     expected_count, actual_count, status,
                     execution_timestamp, creation_timestamp, updated_timestamp)
                VALUES (%s, %s, %s, %s, %s, NULL, NULL, 'RUNNING',
                        %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (execution_id) DO UPDATE SET
                    status = 'RUNNING',
                    updated_timestamp = CURRENT_TIMESTAMP
                """,
                (execution_id, file_path, file_name, file_type, target_table,
                 datetime.utcnow()),
            )
        conn.commit()
        print(f"[Insert] RUNNING record created for execution_id={execution_id}")
    finally:
        conn.close()


def _read_control_file(**context):
    """Read control file -> expected_count, update ingestion_count."""
    conf = context["dag_run"].conf or {}
    control_file_path = conf.get("control_file_path")

    if not control_file_path:
        print("[Control] No control_file_path - skipping validation")
        return None

    control_file_path = _normalize_path(control_file_path)

    if not os.path.exists(control_file_path):
        raise AirflowFailException(f"Control file not found: {control_file_path}")

    properties = {}
    with open(control_file_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                properties[k.strip()] = v.strip()

    record_count_str = properties.get("record_count")
    if not record_count_str:
        raise AirflowFailException(
            f"Control file missing 'record_count' attribute: {control_file_path}"
        )

    try:
        expected = int(record_count_str.strip())
    except ValueError:
        raise AirflowFailException(
            f"Invalid 'record_count' value in control file: '{record_count_str}'"
        )

    print(f"[Control] Expected record count: {expected}")

    execution_id = conf.get("execution_id")
    conn = _get_postgres_conn(database="warehouse")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ingestion_count
                SET expected_count = %s, updated_timestamp = CURRENT_TIMESTAMP
                WHERE execution_id = %s
                """,
                (expected, execution_id),
            )
        conn.commit()
    finally:
        conn.close()

    return expected


def _verify_warehouse_load(**context):
    """Query actual count, compare with expected, update ingestion_count"""
    ti = context["ti"]
    execution_id = ti.xcom_pull(key="execution_id", task_ids="run_beam_ingestion")
    target_table = ti.xcom_pull(key="target_table", task_ids="run_beam_ingestion")
    files_count = ti.xcom_pull(key="files_processed", task_ids="run_beam_ingestion")

    expected_count = ti.xcom_pull(task_ids="read_control_file")

    # Query actual count from Postgres
    conn = _get_postgres_conn(database="warehouse")
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT count(*) FROM {target_table} WHERE execution_id = %s",
                (execution_id,),
            )
            actual_count = cur.fetchone()[0]
            print(f"[Verify] {actual_count} rows from {files_count} file(s) loaded")

            # Update ingestion_count with actual count
            cur.execute(
                """
                UPDATE ingestion_count
                SET actual_count = %s, updated_timestamp = CURRENT_TIMESTAMP
                WHERE execution_id = %s
                """,
                (actual_count, execution_id),
            )
        conn.commit()
    finally:
        conn.close()

    if expected_count is None:
        # No control file - mark as PASSED with actual count
        conn = _get_postgres_conn(database="warehouse")
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE ingestion_count
                    SET status = 'PASSED', updated_timestamp = CURRENT_TIMESTAMP
                    WHERE execution_id = %s
                    """,
                    (execution_id,),
                )
            conn.commit()
        finally:
            conn.close()
        print("[Verify] No expected count - marked as PASSED (BAU)")
        return

    print(f"[Verify] Expected: {expected_count}, Actual: {actual_count}")

    if expected_count != actual_count:
        # Mismatch - record FAILED in ingestion_count but DON'T fail DAG
        failure_reason = (
            f"Record count mismatch! "
            f"Expected: {expected_count}, Actual: {actual_count}, "
            f"Difference: {actual_count - expected_count}"
        )
        conn = _get_postgres_conn(database="warehouse")
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE ingestion_count
                    SET status = 'FAILED', failure_reason = %s,
                        updated_timestamp = CURRENT_TIMESTAMP
                    WHERE execution_id = %s
                    """,
                    (failure_reason, execution_id),
                )
            conn.commit()
        finally:
            conn.close()
        print(f"[Verify] MISMATCH (recorded as FAILED): {failure_reason}")
        return   # DON'T raise - DAG continues as success

    # Match - record PASSED
    conn = _get_postgres_conn(database="warehouse")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ingestion_count
                SET status = 'PASSED', updated_timestamp = CURRENT_TIMESTAMP
                WHERE execution_id = %s
                """,
                (execution_id,),
            )
        conn.commit()
    finally:
        conn.close()
    print(f"[Verify] OK - Record count matches ({expected_count} records)")


def _mark_failed_on_error(context):
    """on_failure_callback - mark ingestion_count as FAILED."""
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    execution_id = conf.get("execution_id")
    if not execution_id:
        return

    exception = context.get("exception")
    failure_reason = str(exception) if exception else "Unknown error"

    try:
        conn = _get_postgres_conn(database="warehouse")
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ingestion_count
                SET status = 'FAILED', failure_reason = %s,
                    updated_timestamp = CURRENT_TIMESTAMP
                WHERE execution_id = %s AND status != 'PASSED'
                """,
                (failure_reason[:1000], execution_id),
            )
        conn.commit()
        conn.close()
        print(f"[FailureHandler] Marked execution_id={execution_id} as FAILED")
    except Exception as e:
        print(f"[FailureHandler] Could not mark failed: {e}")



def _validate_inputs(**context):
    """Check inputs - supports single and multiple files."""
    conf = context["dag_run"].conf or {}

    has_single = "source_file_path" in conf
    has_multiple = "source_file_paths" in conf

    if not (has_single or has_multiple):
        raise AirflowFailException("Need source_file_path OR source_file_paths")

    if has_multiple:
        files = conf["source_file_paths"]
        if not isinstance(files, list) or len(files) == 0:
            raise AirflowFailException("source_file_paths must be non-empty list")
        for f in files:
            if not Path(_normalize_path(f)).is_file():
                raise AirflowFailException(f"File not found: {f}")
        print(f"[validate] OK - {len(files)} files")

    for key in ["schema_file_path", "file_format", "target_table", "execution_id"]:
        if key not in conf or not conf[key]:
            raise AirflowFailException(f"Missing: {key}")

    print(f"[validate] format={conf['file_format']}, table={conf['target_table']}")


def _run_beam_ingestion(**context):
    """Run Beam for each file sequentially."""
    conf = context["dag_run"].conf or {}

    if "source_file_paths" in conf:
        files = conf["source_file_paths"]
    else:
        files = [conf["source_file_path"]]

    print(f"[Beam] Processing {len(files)} file(s) sequentially")

    aes_key = os.environ.get("INGESTION_AES_KEY")
    if not aes_key:
        raise AirflowFailException("INGESTION_AES_KEY not set")

    if not Path(BEAM_JAR).is_file():
        raise AirflowFailException(f"JAR not found: {BEAM_JAR}")

    now = datetime.now(timezone.utc)
    iso_now = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"

    Path(ERRORS_DIR).mkdir(parents=True, exist_ok=True)

    for idx, file_path in enumerate(files, start=1):
        normalized = _normalize_path(file_path)
        print(f"\n=== [{idx}/{len(files)}] {normalized} ===")

        error_file = f"{ERRORS_DIR}/{conf['target_table']}_{conf['execution_id']}_part{idx}.txt"

        cmd = [
            "java", "-jar", BEAM_JAR,
            f"--executionId={conf['execution_id']}",
            f"--sourceFilePath={normalized}",
            f"--schemaFilePath={_normalize_path(conf['schema_file_path'])}",
            f"--fileFormat={conf['file_format']}",
            f"--targetTable={conf['target_table']}",
            f"--warehouseJdbcUrl={WAREHOUSE_JDBC_URL}",
            f"--warehouseUsername={WAREHOUSE_USERNAME}",
            f"--warehousePassword={WAREHOUSE_PASSWORD}",
            f"--errorRecordLocation={error_file}",
            f"--ingestionTimestamp={iso_now}",
            f"--sourceCreationTime={iso_now}",
            "--runner=DirectRunner",
            "--targetParallelism=1",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True,
                                env={**os.environ, "INGESTION_AES_KEY": aes_key})

        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)

        if result.returncode != 0:
            raise AirflowFailException(f"Beam failed for part {idx}")

    context["ti"].xcom_push(key="execution_id", value=conf["execution_id"])
    context["ti"].xcom_push(key="target_table", value=conf["target_table"])
    context["ti"].xcom_push(key="files_processed", value=len(files))



with DAG(
    dag_id="sor_ingestion_phase1",
    description="Phase 1+2+3 SOR ingestion with record count validation",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["ingestion", "phase1", "phase2", "phase3"],
    on_failure_callback=_mark_failed_on_error,
) as dag:


    insert_running = PythonOperator(
        task_id="insert_running_record",
        python_callable=_insert_running_record,
    )


    validate = PythonOperator(
        task_id="validate_inputs",
        python_callable=_validate_inputs,
    )


    run_beam = PythonOperator(
        task_id="run_beam_ingestion",
        python_callable=_run_beam_ingestion,
    )


    read_control = PythonOperator(
        task_id="read_control_file",
        python_callable=_read_control_file,
    )


    verify = PythonOperator(
        task_id="verify_warehouse_load",
        python_callable=_verify_warehouse_load,
    )


    insert_running >> validate >> run_beam >> read_control >> verify




