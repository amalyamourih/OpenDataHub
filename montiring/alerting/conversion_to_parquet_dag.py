import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

src_path = os.path.join(project_root, "src")

sys.path.insert(0, project_root)
sys.path.insert(0, src_path)

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator


from datetime import datetime
from transformation.transforme_with_duckdb.read_object import read_meta_data_
from utils.config import S3_BUCKET, S3_iNPUT_PREFIX, S3_OUTPUT_PREFIX, AWS_CONN_ID
from utils.dictionnaire import DATA_FORMATS

from datetime import datetime, timedelta
from typing import Any
import json

from airflow import DAG
from airflow.decorators import task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.models import Variable

from transformation.transforme_with_duckdb.conversion_to_parquet import (
    convert_csv_to_parquet,
    convert_tsv_to_parquet,
    convert_xls_to_parquet,
    convert_xlsx_to_parquet,
    convert_ods_to_parquet,
    convert_txt_to_parquet,
    convert_dbf_to_parquet,
    convert_parquet_to_parquet,
    convert_pq_to_parquet
)
from transformation.transforme_with_duckdb.conversion_to_parquet import (
    convert_json_to_parquet,
    convert_xml_to_parquet
)

from transformation.transforme_with_duckdb.conversion_to_parquet import (
    convert_shp_to_parquet,
    convert_shz_to_parquet,
    convert_geojson_to_parquet,
    convert_kml_to_parquet,
    convert_kmz_to_parquet,
    convert_gpkg_to_parquet,
    convert_dwg_to_parquet,
    convert_dxf_to_parquet
)

from transformation.transforme_with_duckdb.conversion_to_parquet import (
    convert_tiff_to_parquet,
    convert_tif_to_parquet,
    convert_jp2_to_parquet,
    convert_ecw_to_parquet
)
from transformation.transforme_with_duckdb.conversion_to_parquet import (
    convert_pdf_to_parquet,
    convert_docx_to_parquet,
    convert_odt_to_parquet
)

from transformation.transforme_with_duckdb.conversion_to_parquet import (
    convert_sql_to_parquet,
    convert_db_to_parquet_all_tables
)


from transformation.transforme_with_duckdb.conversion_to_parquet import (
    convert_zip_to_parquet,
    convert_tar_to_parquet,
    convert_tgz_to_parquet,
    convert_gz_to_parquet,
    convert_xz_to_parquet,
    convert_bz2_to_parquet,
    convert_7z_to_parquet,
    convert_rar_to_parquet
)




EXTENSION_MAPPING = {}
for category, extensions in DATA_FORMATS.items():
    for ext, metadata in extensions.items():
        EXTENSION_MAPPING[ext.lower()] = {
            "category": category,
            **metadata
        }


def get_file_extension(file_key: str) -> str:
    if "." not in file_key:
        return ""
    return file_key.rsplit(".", 1)[-1].lower()
   
def get_file_info(file_key: str) -> dict[str, Any]:
    ext = get_file_extension(file_key)
    
    if ext in EXTENSION_MAPPING:
        return {
            "file_key": file_key,
            "extension": ext,
            **EXTENSION_MAPPING[ext]
        }
    
    return {
        "file_key": file_key,
        "extension": ext,
        "category": "unknown",
        "type": "inconnu",
        "description": "Format non reconnu"
    }

def scan_s3_folders() -> list[dict]:

        from airflow.models import Variable
        
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        try:
            processed_files = set(json.loads(
                Variable.get("s3_processed_files", default_var="[]")
            ))
        except json.JSONDecodeError:
            processed_files = set()
        
        new_files = []
        new_files = []
        for category in DATA_FORMATS.keys():
            prefix = f"{S3_iNPUT_PREFIX}{category}/"
            try:
                keys = s3_hook.list_keys(
                    bucket_name=S3_BUCKET,
                    prefix=prefix
                ) or []
                
                for key in keys:
                    if key.endswith("/"):
                        continue
                    if key not in processed_files:
                        file_info = get_file_info(key)
                        file_info["source_folder"] = category
                        new_files.append(file_info)
                        
            except Exception as e:
                print(f"Erreur lors du scan de {prefix}: {e}")
                continue
        
        print(f"{len(new_files)} nouveaux fichiers détectés")
        return new_files
    
 
   
def route_files(files: list[dict]) -> dict[str, list[dict]]:
        routed_files = {
            "tabular": [],
            "geospatial_vector": [],
            "geospatial_raster": [],
            "databases": [],
            "archives": [],
            "documents": []
        }
        
        for file in files:
            category = file["category"]
            if category in routed_files:
                routed_files[category].append(file)
            else:
                print(f"Catégorie inconnue: {category} pour le fichier {file['file_key']}")

        return routed_files        
        
        
# TODO : Refaire le code trop repetitive
def process_tabular(routed_files: dict) -> list[str]:
        files = routed_files.get("tabular", [])
        processed = []
        new_parquet_files = []
        for file in files:
            key = file["file_key"]
            ext = file["extension"]
            print(f"Traitement tabulaire: {key} ({ext})")
            match ext:
                case "csv":
                    new_parquet_files.append(convert_csv_to_parquet(key))
                case "tsv":
                    new_parquet_files.append(convert_tsv_to_parquet(key))
                case "xls":    
                    new_parquet_files.append(convert_xls_to_parquet(key))
                case "xlsx":
                    new_parquet_files.append(convert_xlsx_to_parquet(key))
                case "ods":
                    new_parquet_files.append(convert_ods_to_parquet(key))
                case "txt":
                    new_parquet_files.append(convert_txt_to_parquet(key))   
                case "dbf":
                    new_parquet_files.append(convert_dbf_to_parquet(key))
                case "parquet" | "pq":
                    new_parquet_files.append(convert_parquet_to_parquet(key))
                case _:
                    print(f"Format tabulaire non pris en charge: {ext} pour le fichier {key}")
            processed.append(key)
        return new_parquet_files, processed
    
def process_geospatial_vector(routed_files: dict) -> list[str]:
        """Traite les fichiers géospatiaux vecteur (Shapefile, GeoJSON, etc.)."""
        files = routed_files.get("geospatial_vector", [])
        processed = []
        new_parquet_files = []
        for file in files:
            key = file["file_key"]
            ext = file["extension"]
            print(f"Traitement vecteur: {key} ({ext})")
            match ext:
                case "shp":
                    new_parquet_files.append(convert_shp_to_parquet(key))
                case "shz":
                    new_parquet_files.append(convert_shz_to_parquet(key))
                case "geojson":
                    new_parquet_files.append(convert_geojson_to_parquet(key))
                case "kml":
                    new_parquet_files.append(convert_kml_to_parquet(key))
                case "kmz":
                    new_parquet_files.append(convert_kmz_to_parquet(key))
                case "gpkg":
                    new_parquet_files.append(convert_gpkg_to_parquet(key))
                case "dwg":
                    new_parquet_files.append(convert_dwg_to_parquet(key))
                case "dxf":
                    new_parquet_files.append(convert_dxf_to_parquet(key))
                case _:
                    print(f"Format géospatial vecteur non pris en charge: {ext} pour le fichier {key}")

            
            
            processed.append(key)
        
        return new_parquet_files, processed
    

def process_geospatial_raster(routed_files: dict) -> list[str]:
        files = routed_files.get("geospatial_raster", [])
        processed = []
        
        for file in files:
            key = file["file_key"]
            ext = file["extension"]
            print(f"Traitement raster: {key} ({ext})")
            
            # TODO: Création de tuiles, extraction metadata, etc.
            
            processed.append(key)
        
        return processed
    
    

def process_databases(routed_files: dict) -> list[str]:
        """Traite les fichiers base de données (SQL, SQLite)."""
        files = routed_files.get("databases", [])
        processed = []
        
        for file in files:
            key = file["file_key"]
            ext = file["extension"]
            print(f"Traitement base: {key} ({ext})")
            
            # TODO: Import dans DWH, extraction tables, etc.
            
            processed.append(key)
        
        return processed

def process_archives(routed_files: dict) -> list[str]:
        """Traite les archives (ZIP, TAR, etc.) - décompression et re-routing."""
        files = routed_files.get("archives", [])
        processed = []
        
        for file in files:
            key = file["file_key"]
            ext = file["extension"]
            print(f"📦 Traitement archive: {key} ({ext})")
            
            # TODO: Décompression et placement des fichiers extraits
            # dans les bons dossiers pour re-traitement
            
            processed.append(key)
        
        return processed
    
 
def process_documents(routed_files: dict) -> list[str]:
        """Traite les documents (PDF, DOCX, etc.)."""
        files = routed_files.get("documents", [])
        processed = []
        
        for file in files:
            key = file["file_key"]
            ext = file["extension"]
            print(f"📄 Traitement document: {key} ({ext})")
            
            # TODO: Extraction texte, OCR, etc.
            
            processed.append(key)
        
        return processed
    



with DAG(
    dag_id="conversion_to_parquet_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    