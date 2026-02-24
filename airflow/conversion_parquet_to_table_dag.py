import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator 


import snowflake.connector

from ingestion.s3.get_parquets_files import list_s3_keys
from utils.get_table_name import get_table_name_from_key
from utils.config import S3_BUCKET


def _create_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        role=os.getenv("SNOWFLAKE_ROLE", None),
    )


def load_parquets_to_snowflake(**context):

    prefix = os.getenv("S3_OUTPUT_PREFIX", "parquets_files/")
    stage_name = os.getenv("SNOWFLAKE_STAGE", "MY_S3_STAGE")
    file_format = os.getenv("SNOWFLAKE_PARQUET_FORMAT", "MY_PARQUET_FORMAT")

    conx = _create_snowflake_connection()
    cursor = conx.cursor()

    try:
        keys = list_s3_keys(S3_BUCKET, prefix=prefix)
        parquet_keys = [k for k in keys if k.lower().endswith(".parquet")]

        print(f"{len(parquet_keys)} fichiers parquet trouvés dans S3 sous prefix={prefix}")

        for key in parquet_keys:
            table_name = get_table_name_from_key(key).upper()
            stage_path = f"@{stage_name}/{key}"

            try:
                print(f"{stage_path} -> {table_name}")

                cursor.execute(f"""
                    CREATE OR REPLACE TABLE "{table_name}"
                    USING TEMPLATE (
                        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                        FROM TABLE(
                            INFER_SCHEMA(
                                LOCATION => '{stage_path}',
                                FILE_FORMAT => '{file_format}'
                            )
                        )
                    );
                """)

                cursor.execute(f"""
                    COPY INTO "{table_name}"
                    FROM '{stage_path}'
                    FILE_FORMAT = (FORMAT_NAME = '{file_format}')
                    MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;
                """)

                print(f"OK: {table_name}")

            except Exception as e:
                # On log et on continue (sinon 1 fichier casse tout le run)
                print(f"Erreur sur {key} -> {table_name}: {e}")

    finally:
        try:
            cursor.close()
        except Exception:
            pass
        conx.close()


with DAG(
    dag_id="conversion_parquet_to_table_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["snowflake", "warehouse", "parquet", "load"],
) as dag:
    start = EmptyOperator(task_id="start")

    load = PythonOperator(
        task_id="load_parquets_to_snowflake",
        python_callable=load_parquets_to_snowflake,
    )

    
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id="transformation_dbt_orchestrated",  
        wait_for_completion=False,   
        reset_dag_run=True,         
        poke_interval=30,
    )


    end = EmptyOperator(task_id="end")

    start >> load >> trigger_dbt >> end