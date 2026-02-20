import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

from datetime import datetime
from typing import Any
import json

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from utils.config import S3_BUCKET, S3_iNPUT_PREFIX, S3_OUTPUT_PREFIX, AWS_CONN_ID
from utils.dictionnaire import DATA_FORMATS

# Import des fonctions de conversion depuis les vrais modules
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.csv import convert_csv_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.tsv import convert_tsv_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.xls import convert_xls_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.xlsx import convert_xlsx_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.ods import convert_ods_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.txt import convert_txt_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.dbf import convert_dbf_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.parquet import convert_parquet_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.pq import convert_pq_to_parquet

from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_vector.shp import convert_shp_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_vector.shz import convert_shz_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_vector.geojson import convert_geojson_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_vector.kml import convert_kml_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_vector.kmz import convert_kmz_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_vector.gpkg import convert_gpkg_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_vector.dwg import convert_dwg_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_vector.dxf import convert_dxf_to_parquet

from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_raster.tiff import convert_tiff_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_raster.tif import convert_tif_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_raster.jp2 import convert_jp2_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_raster.ecw import convert_ecw_to_parquet

from transformation.transformat_files_to_parquet.convert_to_parquet.documents.pdf import convert_pdf_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.documents.docx import convert_docx_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.documents.odt import convert_odt_to_parquet

from transformation.transformat_files_to_parquet.convert_to_parquet.databases.sql import convert_sql_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.databases.db import convert_db_to_parquet_all_tables

from transformation.transformat_files_to_parquet.convert_to_parquet.others.json import convert_json_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.others.xml import convert_xml_to_parquet

from transformation.transformat_files_to_parquet.convert_to_parquet.archive.zip import convert_zip_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.archive.tar import convert_tar_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.archive.tgz import convert_tgz_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.archive.gz import convert_gz_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.archive.xz import convert_xz_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.archive.bz2 import convert_bz2_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.archive._7z import convert_7z_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.archive.rar import convert_rar_to_parquet



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


@task
def scan_s3_folders() -> list[dict]:
 
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    try:
        processed_files = set(json.loads(
            Variable.get("s3_processed_files", default_var="[]")
        ))
    except json.JSONDecodeError:
        processed_files = set()

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


@task
def route_files(files: list[dict]) -> dict[str, list[dict]]:
    routed_files = {
        "tabular": [],
        "geospatial_vector": [],
        "geospatial_raster": [],
        "databases": [],
        "archives": [],
        "documents": [],
        "others": []
    }

    for file in files:
        category = file["category"]
        if category in routed_files:
            routed_files[category].append(file)
        else:
            print(f"Catégorie inconnue: {category} pour le fichier {file['file_key']}")

    # Affiche les statistiques
    for cat, file_list in routed_files.items():
        if file_list:
            print(f"{cat}: {len(file_list)} fichiers")

    return routed_files


@task
def process_tabular(routed_files: dict) -> list[str]:
    files = routed_files.get("tabular", [])
    processed = []

    for file in files:
        key = file["file_key"]
        ext = file["extension"]

        try:
            print(f"Traitement tabulaire: {key} ({ext})")

            match ext:
                case "csv":
                    convert_csv_to_parquet(key)
                case "tsv":
                    convert_tsv_to_parquet(key)
                case "xls":
                    convert_xls_to_parquet(key)
                case "xlsx":
                    convert_xlsx_to_parquet(key)
                case "ods":
                    convert_ods_to_parquet(key)
                case "txt":
                    convert_txt_to_parquet(key)
                case "dbf":
                    convert_dbf_to_parquet(key)
                case "parquet":
                    convert_parquet_to_parquet(key)
                case "pq":
                    convert_pq_to_parquet(key)
                case _:
                    print(f"Format tabulaire non pris en charge: {ext}")
                    continue

            processed.append(key)
            print(f"Converti: {key}")

        except Exception as e:
            print(f"Erreur pour {key}: {e}")

    return processed


@task
def process_geospatial_vector(routed_files: dict) -> list[str]:
    files = routed_files.get("geospatial_vector", [])
    processed = []

    for file in files:
        key = file["file_key"]
        ext = file["extension"]

        try:
            print(f"Traitement vecteur: {key} ({ext})")

            match ext:
                case "shp":
                    convert_shp_to_parquet(key)
                case "shz":
                    convert_shz_to_parquet(key)
                case "geojson":
                    convert_geojson_to_parquet(key)
                case "kml":
                    convert_kml_to_parquet(key)
                case "kmz":
                    convert_kmz_to_parquet(key)
                case "gpkg":
                    convert_gpkg_to_parquet(key)
                case "dwg":
                    convert_dwg_to_parquet(key)
                case "dxf":
                    convert_dxf_to_parquet(key)
                case _:
                    print(f"Format vecteur non pris en charge: {ext}")
                    continue

            processed.append(key)
            print(f"Converti: {key}")

        except Exception as e:
            print(f"Erreur pour {key}: {e}")

    return processed


@task
def process_geospatial_raster(routed_files: dict) -> list[str]:
    files = routed_files.get("geospatial_raster", [])
    processed = []

    for file in files:
        key = file["file_key"]
        ext = file["extension"]

        try:
            print(f"Traitement raster: {key} ({ext})")

            match ext:
                case "tiff":
                    convert_tiff_to_parquet(key)
                case "tif":
                    convert_tif_to_parquet(key)
                case "jp2":
                    convert_jp2_to_parquet(key)
                case "ecw":
                    convert_ecw_to_parquet(key)
                case _:
                    print(f"Format raster non pris en charge: {ext}")
                    continue

            processed.append(key)
            print(f"Converti: {key}")

        except Exception as e:
            print(f"Erreur pour {key}: {e}")

    return processed


@task
def process_databases(routed_files: dict) -> list[str]:
    files = routed_files.get("databases", [])
    processed = []

    for file in files:
        key = file["file_key"]
        ext = file["extension"]

        try:
            print(f"Traitement base: {key} ({ext})")

            match ext:
                case "sql":
                    convert_sql_to_parquet(key)
                case "db":
                    convert_db_to_parquet_all_tables(key)
                case _:
                    print(f"Format base de données non pris en charge: {ext}")
                    continue

            processed.append(key)
            print(f"Converti: {key}")

        except Exception as e:
            print(f"Erreur pour {key}: {e}")

    return processed


@task
def process_archives(routed_files: dict) -> list[str]:
    files = routed_files.get("archives", [])
    processed = []

    for file in files:
        key = file["file_key"]
        ext = file["extension"]

        try:
            print(f"Traitement archive: {key} ({ext})")

            match ext:
                case "zip":
                    convert_zip_to_parquet(key)
                case "tar":
                    convert_tar_to_parquet(key)
                case "tgz":
                    convert_tgz_to_parquet(key)
                case "gz":
                    convert_gz_to_parquet(key)
                case "xz":
                    convert_xz_to_parquet(key)
                case "bz2":
                    convert_bz2_to_parquet(key)
                case "7z":
                    convert_7z_to_parquet(key)
                case "rar":
                    convert_rar_to_parquet(key)
                case _:
                    print(f"Format archive non pris en charge: {ext}")
                    continue

            processed.append(key)
            print(f"Converti: {key}")

        except Exception as e:
            print(f"Erreur pour {key}: {e}")

    return processed


@task
def process_documents(routed_files: dict) -> list[str]:
    files = routed_files.get("documents", [])
    processed = []

    for file in files:
        key = file["file_key"]
        ext = file["extension"]

        try:
            print(f"Traitement document: {key} ({ext})")

            match ext:
                case "pdf":
                    convert_pdf_to_parquet(key)
                case "docx":
                    convert_docx_to_parquet(key)
                case "odt":
                    convert_odt_to_parquet(key)
                case _:
                    print(f"Format document non pris en charge: {ext}")
                    continue

            processed.append(key)
            print(f"Converti: {key}")

        except Exception as e:
            print(f"Erreur pour {key}: {e}")

    return processed


@task
def process_others(routed_files: dict) -> list[str]:
    files = routed_files.get("others", [])
    processed = []

    for file in files:
        key = file["file_key"]
        ext = file["extension"]

        try:
            print(f"Traitement autre: {key} ({ext})")

            match ext:
                case "json":
                    convert_json_to_parquet(key)
                case "xml":
                    convert_xml_to_parquet(key)
                case _:
                    print(f"Format non pris en charge: {ext}")
                    continue

            processed.append(key)
            print(f"Converti: {key}")

        except Exception as e:
            print(f"Erreur pour {key}: {e}")

    return processed


@task
def mark_files_as_processed(processed_lists: list[list[str]]):
    all_processed = []
    for proc_list in processed_lists:
        all_processed.extend(proc_list)

    if not all_processed:
        print("Aucun fichier traité")
        return

    try:
        processed_files = set(json.loads(
            Variable.get("s3_processed_files", default_var="[]")
        ))
    except json.JSONDecodeError:
        processed_files = set()

    processed_files.update(all_processed)
    Variable.set("s3_processed_files", json.dumps(list(processed_files)))

    print(f"{len(all_processed)} fichiers marqués comme traités")
    print(f"Total de fichiers traités: {len(processed_files)}")



with DAG(
    dag_id="conversion_to_parquet_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,  
    catchup=False,
    description="Pipeline de conversion des fichiers S3 en format Parquet",
    tags=["etl", "parquet", "s3", "transformation", "conversion"],
) as dag:

    start = EmptyOperator(task_id="start")

    scanned_files = scan_s3_folders()
    routed = route_files(scanned_files)


    tabular_processed = process_tabular(routed)
    vector_processed = process_geospatial_vector(routed)
    raster_processed = process_geospatial_raster(routed)
    db_processed = process_databases(routed)
    archive_processed = process_archives(routed)
    doc_processed = process_documents(routed)
    other_processed = process_others(routed)

    mark_processed = mark_files_as_processed([
        tabular_processed,
        vector_processed,
        raster_processed,
        db_processed,
        archive_processed,
        doc_processed,
        other_processed
    ])

    end = EmptyOperator(task_id="end")

    start >> scanned_files >> routed

    routed >> [
        tabular_processed,
        vector_processed,
        raster_processed,
        db_processed,
        archive_processed,
        doc_processed,
        other_processed
    ] >> mark_processed >> end
