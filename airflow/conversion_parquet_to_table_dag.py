import os
import sys

# --- Imports Airflow ---
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from pathlib import Path

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path: sys.path.insert(0, project_root)
# --- Imports du projet ---
from ingestion.ingestion_to_warehouse.ingestion import (
    ingest_warehouse,
    create_duckdb_connection,
    build_db_path,
    prepare_database_file,
    load_config
)
from utils.config import S3_BUCKET

# --- Fonctions Airflow ---
def load_config_task(ti, **kwargs):
    config = load_config()
    ti.xcom_push(key='config', value=config)
    print("Configuration chargée")
    print(project_root)

def build_db_path_task(ti, **kwargs):
    db_path = db_path = os.path.join(project_root, "warehouse", "warehouse.duckdb")
    ti.xcom_push(key='db_path', value=str(db_path))
    print(f"DB Path: {db_path}")

def prepare_database_file_task(ti, **kwargs):
    db_path = Path(ti.xcom_pull(key='db_path', task_ids='build_db_path'))
    prepare_database_file(db_path)
    print("Fichier de base de données préparé")

def create_duckdb_connection_task(ti, **kwargs):
    db_path = Path(ti.xcom_pull(key='db_path', task_ids='build_db_path'))
    _ = create_duckdb_connection(db_path)
    ti.xcom_push(key='conx_open', value=True)
    print("Connexion DuckDB créée")

def ingest_warehouse_task(ti, **kwargs):
    config = ti.xcom_pull(key='config', task_ids='load_config')
    db_path = Path(ti.xcom_pull(key='db_path', task_ids='build_db_path'))
    prefix = config["s3"]["prefix"]
    formats = tuple(f".{fmt}" for fmt in config["formats"])
    
    conx = create_duckdb_connection(db_path)
    ingest_warehouse(conx, S3_BUCKET, prefix, formats)
    conx.close()
    print("Ingestion terminée !")

# --- DAG ---
with DAG(
    dag_id="ingestion_warehouse_orchestrated",
    start_date=datetime(2026, 2, 13),
    schedule="@daily",
    catchup=False,
    tags=["warehouse", "ingestion"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    t1 = PythonOperator(task_id="load_config", python_callable=load_config_task)
    t2 = PythonOperator(task_id="build_db_path", python_callable=build_db_path_task)
    t3 = PythonOperator(task_id="prepare_database_file", python_callable=prepare_database_file_task)
    t4 = PythonOperator(task_id="create_duckdb_connection", python_callable=create_duckdb_connection_task)
    t5 = PythonOperator(task_id="ingest_warehouse", python_callable=ingest_warehouse_task)

    # --- Chaînage ---
    start >> t1 >> t2 >> t3 >> t4 >> t5 >> end