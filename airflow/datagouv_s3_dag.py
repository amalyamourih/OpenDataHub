import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

src_path = os.path.join(project_root, "src")

sys.path.insert(0, project_root)
sys.path.insert(0, src_path)

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator 

from datetime import datetime
from ingestion.ingestion_to_S3.datagouv_client import get_dataset_metadata, find_resource_for_format,list_last_updated_dataset_slugs
from ingestion.ingestion_to_S3.downloader import download_file
from ingestion.ingestion_to_S3.s3_uploader import upload_folder_to_s3
from utils.config import S3_BUCKET, AWS_REGION, DATASET_SLUG
from utils.dictionnaire import DATA_FORMATS
import logging
log = logging.getLogger("airflow.task")


# --- ajoute le helper sanitize + guard ---
import re
from airflow.exceptions import AirflowFailException

def _sanitize_run_id(s: str) -> str:
    return re.sub(r"[^\w\-]", "_", s or "")

def init_run_context(ti, **context):
    # correlation_id = run_id du DAG ingestion (unique)
    cid = context["dag_run"].run_id
    ti.xcom_push(key="correlation_id", value=cid)
    ti.xcom_push(key="parent_dag_id", value=context["dag"].dag_id)
    ti.xcom_push(key="parent_run_id", value=context["dag_run"].run_id)
    print(f"[chain] correlation_id={cid}")

def _get_tmp_path_from_cid(correlation_id: str) -> str:
    safe = _sanitize_run_id(correlation_id)
    return f"/tmp/data_temp/{safe}/"

def download_resource(ti, **context):
    selected = ti.xcom_pull(key="selected", task_ids="select_ressource")
    if not selected:
        raise ValueError("XCom 'selected' vide !")

    correlation_id = ti.xcom_pull(key="correlation_id", task_ids="init_run_context")
    if not correlation_id:
        raise ValueError("Missing correlation_id from init_run_context")

    tmp_path = _get_tmp_path_from_cid(correlation_id)

    for item in selected:
        slug = item["slug"]
        resources = item["resources"]
        download_file(resources, dest_path_data_temp=f"{tmp_path}{slug}/")

    ti.xcom_push(key="tmp_path", value=tmp_path)
    print(f"[chain] tmp_path={tmp_path} correlation_id={correlation_id}")






def fetch_metadata(ti):
    log.info("Début fetch_metadata")
    limit = 3  # à ajuster selon le nombre de datasets que tu veux   !!!!!!!!!!!!!!!!!!!!!!
    slugs = list_last_updated_dataset_slugs(limit=limit)
    if not slugs:
        raise ValueError(f"Impossible de récupérer les {limit} derniers datasets mis à jour (liste vide).")
    ti.xcom_push(key='dataset_slugs',value=slugs)

    log.info("Métadonnées récupérées")


def select_resource(ti):
    slugs = ti.xcom_pull(key="dataset_slugs", task_ids="fetch_metadata")
    if not slugs:
        raise ValueError("XCom 'slugs' vide. Vérifie la tâche fetch_metadata.")

    selected = []
    for slug in slugs:
        dataset_meta = get_dataset_metadata(slug)
        resources = find_resource_for_format(dataset_meta)
        if resources:
            selected.append({"slug": slug, "resources": resources})

    if not selected:
        raise ValueError("Aucune ressource correspondant aux formats supportés n'a été trouvée pour les datasets récupérés.")
    ti.xcom_push(key="selected", value=selected)
    print(f"{len(selected)} datasets ont au moins une ressource supportée")

import re

def _get_tmp_path(ti) -> str:
    safe_run_id = re.sub(r"[^\w\-]", "_", ti.run_id)
    return f"/tmp/data_temp/{safe_run_id}/"


def download_resource(ti):
    selected = ti.xcom_pull(key="selected", task_ids="select_ressource")
    if not selected:
        raise ValueError("XCom 'selected' vide !")

    tmp_path = _get_tmp_path(ti)   # ← chemin isolé par run

    for item in selected:
        slug      = item["slug"]
        resources = item["resources"]
        download_file(resources, dest_path_data_temp=f"{tmp_path}{slug}/")

    ti.xcom_push(key="tmp_path", value=tmp_path)
    print(f"Fichiers téléchargés dans : {tmp_path}")


def upload_to_s3(ti):
    tmp_path = ti.xcom_pull(key="tmp_path", task_ids="download_ressource")
    if not tmp_path:
        raise ValueError("XCom 'tmp_path' vide ! Vérifie download_ressource")

    upload_folder_to_s3(tmp_path, S3_BUCKET, AWS_REGION)
    print(f"Upload terminé depuis : {tmp_path}")


def monitor_ingestion(ti):
    selected = ti.xcom_pull(key="selected", task_ids="select_ressource") or []
    nb_datasets = len(selected)
    nb_files    = sum(len(x["resources"]) for x in selected)

    log.info("Monitoring: datasets=%s files=%s", nb_datasets, nb_files)

    if nb_files == 0:
        raise ValueError("Monitoring: 0 fichier sélectionné → ingestion invalide")

with DAG(
    dag_id="ingestion_dag",
    start_date=datetime(2026, 1, 1),
    schedule="0 9 * * *",   
    catchup=False,
    description="Ingestion quotidienne des données data.gouv.fr vers S3",
    tags=["ingestion", "s3", "datagouv"],
) as dag:

    start = EmptyOperator(
	task_id="start",

    )
    
    t0 = PythonOperator(
    task_id="init_run_context", 
        python_callable=init_run_context
    )
    
    t1 = PythonOperator(
	task_id="fetch_metadata",
        python_callable=fetch_metadata,
    )

    t2 = PythonOperator(
	task_id="select_ressource",
        python_callable=select_resource,

    )

    t3 = PythonOperator(
	task_id="download_ressource", 
	python_callable=download_resource,
    )

    t4 = PythonOperator(
	task_id="Upload_s3",
        python_callable=upload_to_s3,
    )

    monitor = PythonOperator(
        task_id="monitor_ingestion",
        python_callable=monitor_ingestion,
    )
    
    trigger_conversion = TriggerDagRunOperator(
        task_id="trigger_conversion_dag",
        trigger_dag_id="conversion_to_parquet_dag",
        conf={
            "correlation_id": "{{ ti.xcom_pull(task_ids='init_run_context', key='correlation_id') }}",
            "parent_dag_id": "{{ dag.dag_id }}",
            "parent_run_id": "{{ dag_run.run_id }}",
        },

        trigger_run_id="chain__{{ ti.xcom_pull(task_ids='init_run_context', key='correlation_id') }}__conversion",
        wait_for_completion=False,
        reset_dag_run=False, 
    )

    end = EmptyOperator(
	task_id="end"
    )



# Chaînage (exemple)
start >> t0 >> t1 >> t2 >> t3 >> t4 >> monitor >> trigger_conversion >> end
