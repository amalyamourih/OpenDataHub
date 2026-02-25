import os
import sys

# --- Imports Airflow ---
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from pathlib import Path


project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from transformation.transforme_with_dbt.dbt.scripts.source import (
    get_duckdb_connection,
    get_table_names,
    build_sources_dict,
    save_yaml
)

from transformation.transforme_with_dbt.dbt.scripts.generate_sql_bronze import (
    load_sources_yaml,
    generate_bronze_sql_files
)

from transformation.transforme_with_dbt.dbt.scripts.generate_sql_silver import (
    generate_silver_models
)

from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

def require_correlation_id_task(ti, **context):
    conf = (context["dag_run"].conf or {})
    cid = conf.get("correlation_id")
    if not cid:
        raise AirflowFailException("Missing correlation_id in dag_run.conf")
    ti.xcom_push(key="correlation_id", value=cid)
    print(f"[chain] correlation_id={cid}")

guard = PythonOperator(
    task_id="guard_correlation",
    python_callable=require_correlation_id_task,
)



def generate_dbt_sources_task(ti, **kwargs):
    warehouse_path = os.path.join(project_root, "warehouse", "warehouse.duckdb")

    conx = get_duckdb_connection(warehouse_path)
    tables = get_table_names(conx)
    sources_dict = build_sources_dict(tables)

    source_yaml_path = os.path.join(
        project_root,
        "transformation",
        "transforme_with_dbt",
        "dbt",
        "models",
        "source",
        "source.yml"
    )

    save_yaml(sources_dict, source_yaml_path)
    conx.close()

    ti.xcom_push(key="source_yaml_path", value=source_yaml_path)

    print("Fichier source.yml généré avec succès")


def generate_bronze_models_task(ti, **kwargs):
    source_yaml_path = ti.xcom_pull(
        key="source_yaml_path",
        task_ids="generate_dbt_sources"
    )

    bronze_folder = os.path.join(
        project_root,
        "transformation",
        "transforme_with_dbt",
        "dbt",
        "models",
        "bronze"
    )

    sources = load_sources_yaml(source_yaml_path)
    generate_bronze_sql_files(sources, bronze_folder)

    ti.xcom_push(key="bronze_folder", value=bronze_folder)

    print("Modèles Bronze générés avec succès")


def generate_silver_models_task(ti, **kwargs):
    bronze_folder = ti.xcom_pull(
        key="bronze_folder",
        task_ids="generate_bronze_models"
    )

    silver_folder = os.path.join(
        project_root,
        "transformation",
        "transforme_with_dbt",
        "dbt",
        "models",
        "silver"
    )

    generate_silver_models(bronze_folder, silver_folder)

    print("Modèles Silver générés avec succès")


with DAG(
    dag_id="transformation_dbt_orchestrated",
    start_date=datetime(2026, 2, 13),
    schedule=None,
    catchup=False,
    tags=["dbt", "transformation", "warehouse"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    t1 = PythonOperator(
        task_id="generate_dbt_sources",
        python_callable=generate_dbt_sources_task
    )

    t2 = PythonOperator(
        task_id="generate_bronze_models",
        python_callable=generate_bronze_models_task
    )

    t3 = PythonOperator(
        task_id="generate_silver_models",
        python_callable=generate_silver_models_task
    )

    

    start >> guard >> t1 >> t2 >> t3 >> end