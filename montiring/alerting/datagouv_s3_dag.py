import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

src_path = os.path.join(project_root, "src")

sys.path.insert(0, project_root)
sys.path.insert(0, src_path)

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator


from datetime import datetime
from ingestion.ingestion_to_S3.datagouv_client import get_dataset_metadata, find_resource_for_format
from ingestion.ingestion_to_S3.downloader import download_file
from ingestion.ingestion_to_S3.s3_uploader import upload_folder_to_s3
from utils.config import S3_BUCKET, AWS_REGION, DATASET_SLUG
from utils.dictionnaire import DATA_FORMATS

# Récupération des métadonnées
def fetch_metadata(ti):
    dataset_meta = get_dataset_metadata(DATASET_SLUG)
    ti.xcom_push(key='dataset_meta',value=dataset_meta)
    print("Métadonnées récupérées")

#Récupération des sources de données à partir des métadonnées
def select_resource(ti):
    dataset_meta = ti.xcom_pull(key='dataset_meta', task_ids='fetch_metadata')
    resource = find_resource_for_format(dataset_meta)
    if not resource:
      raise ValueError("Aucune ressource XLS/XLSX troouvée")
    ti.xcom_push(key='resource', value=resource)
    print(f"Ressource sélectionnée")

# TODO : EVITER LES FICHIERS TEMPORAIRES DANS LE REPO UTILISER LA MACHINE VIRTUELLE 
#Téléchargement temporaire des sources de données dans un dossier
def download_resource(ti):
    resource = ti.xcom_pull(key='resource',task_ids='select_ressource')
    if not resource:
        raise ValueError("Resource est vide ! Vérifie le XCom de la tâche précédente")
    download_file(resource)
    print("Fichier téléchargé")

# Stocker les sources de données dans le service S3
def upload_to_s3(ti):
    upload_folder_to_s3("data_temp", S3_BUCKET, AWS_REGION)
    print("Fichier upload sur S3")


# TODO 

with DAG(
    dag_id="orchestration_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    start = EmptyOperator(
	task_id="start",

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

    end = EmptyOperator(
	task_id="end"
    )


start >> t1 >> t2 >> t3 >> t4 >> end
