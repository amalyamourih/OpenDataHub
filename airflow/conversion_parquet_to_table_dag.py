import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from ingestion.ingestion_to_warehouse.ingestion import (
    create_snowflake_connection,
    load_parquets_to_snowflake,
)


def guard_correlation(**context):
    conf = (context["dag_run"].conf or {})
    cid = conf.get("correlation_id")
    if not cid:
        raise AirflowFailException("Missing correlation_id in dag_run.conf")
    return cid


def load_task(ti, **context):
    cid = ti.xcom_pull(task_ids="guard_correlation")
    print(f"[chain] correlation_id={cid} starting Snowflake load")

    prefix = os.getenv("S3_OUTPUT_PREFIX", "parquets_files/")
    stage_name = os.getenv("SNOWFLAKE_STAGE", "MY_S3_STAGE")
    file_format = os.getenv("SNOWFLAKE_PARQUET_FORMAT", "MY_PARQUET_FORMAT")

    conx = create_snowflake_connection()
    try:
        tables = load_parquets_to_snowflake(
            conx,
            prefix=prefix,
            stage_name=stage_name,
            file_format=file_format,
            formats=(".parquet",),
        )
        print(f"[chain] correlation_id={cid} loaded_tables={len(tables)}")
    finally:
        conx.close()


with DAG(
    dag_id="conversion_parquet_to_table_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["snowflake", "warehouse", "parquet", "load"],
) as dag:

    start = EmptyOperator(task_id="start")

    guard = PythonOperator(
        task_id="guard_correlation",
        python_callable=guard_correlation,
    )

    load = PythonOperator(
        task_id="load_parquets_to_snowflake",
        python_callable=load_task,
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id="transformation_dbt_orchestrated",
        conf={
            "correlation_id": "{{ ti.xcom_pull(task_ids='guard_correlation') }}",
            "parent_dag_id": "{{ dag.dag_id }}",
            "parent_run_id": "{{ dag_run.run_id }}",
        },
        trigger_run_id="chain__{{ ti.xcom_pull(task_ids='guard_correlation') }}__dbt",
        wait_for_completion=False,
        reset_dag_run=False,
        poke_interval=30,
    )

    end = EmptyOperator(task_id="end")

    start >> guard >> load >> trigger_dbt >> end