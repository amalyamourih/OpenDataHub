import pandas as pd 
import boto3
from src.ingestion_to_S3.config import S3_BUCKET
import io
import os
import sqlite3
import tempfile
import geopandas as gpd
import zipfile
import ezdxf

def convert_csv_to_parquet(path_to_csv_key):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_csv_key)
    csv_content = obj['Body'].read()
    df = pd.read_csv(io.BytesIO(csv_content))
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0) 

    csv_filename = path_to_csv_key.split("/")[-1].replace(".csv", ".parquet")
    parquet_key = f"parquets_files/{csv_filename}"

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=parquet_key,
        Body=parquet_buffer.getvalue()
    )

    print(f"Fichier Parquet chargé sur S3")

def convert_tsv_to_parquet(path_to_tsv_key):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_tsv_key)
    tsv_content = obj['Body'].read()
    df = pd.read_csv(io.BytesIO(tsv_content), sep='\t')
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0) 

    tsv_filename = path_to_tsv_key.split("/")[-1].replace(".tsv", ".parquet")
    parquet_key = f"parquets_files/{tsv_filename}"

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=parquet_key,
        Body=parquet_buffer.getvalue()
    )

    print(f"Fichier Parquet chargé sur S3") 


def convert_db_to_parquet_all_tables(path_to_db_key):
    s3_client = boto3.client('s3')

    #Télécharger le .db
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_db_key)
    db_content = obj['Body'].read()

    #Fichier temporaire
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_db:
        tmp_db.write(db_content)
        tmp_db_path = tmp_db.name

    try:
        conn = sqlite3.connect(tmp_db_path)

        #Récupérer les tables
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type='table'
              AND name NOT LIKE 'sqlite_%';
        """)
        tables = [row[0] for row in cursor.fetchall()]

        # Convertir chaque table
        for table in tables:
            df = pd.read_sql(f"SELECT * FROM {table}", conn)

            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)

            parquet_key = f"parquets_files/{table}.parquet"

            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=parquet_key,
                Body=parquet_buffer.getvalue()
            )

            print(f"Table {table} → {parquet_key}")

    finally:
        conn.close()
        os.remove(tmp_db_path)

def convert_xlsx_to_parquet(path_to_xlsx_key):
    s3_client = boto3.client('s3')

    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_xlsx_key)
    xlsx_content = obj['Body'].read()

    excel_file = pd.ExcelFile(io.BytesIO(xlsx_content))

    for sheet_name in excel_file.sheet_names:
        df = pd.read_excel(excel_file, sheet_name=sheet_name)

        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        parquet_key = f"parquets_files/{sheet_name}.parquet"

        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=parquet_key,
            Body=parquet_buffer.getvalue()
        )

        print(f"Feuille '{sheet_name}' convertie en Parquet")

def convert_xls_to_parquet(path_to_xls_key):
    s3_client = boto3.client('s3')

    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_xls_key)
    xls_content = obj['Body'].read()

    excel_file = pd.ExcelFile(io.BytesIO(xls_content), engine="xlrd")  # moteur pour .xls

    for sheet_name in excel_file.sheet_names:
        df = pd.read_excel(excel_file, sheet_name=sheet_name, engine="xlrd")
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str)


        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        parquet_key = f"parquets_files/{sheet_name}.parquet"

        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=parquet_key,
            Body=parquet_buffer.getvalue()
        )

        print(f"Feuille '{sheet_name}' convertie en Parquet")

    
def convert_sql_to_parquet(path_to_sql_key):
    s3_client = boto3.client('s3')

    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_sql_key)
    sql_content = obj['Body'].read().decode("utf-8")

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_db:
        tmp_db_path = tmp_db.name

    try:
        conn = sqlite3.connect(tmp_db_path)
        cursor = conn.cursor()

        cursor.executescript(sql_content)
        conn.commit()

        cursor.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type='table'
              AND name NOT LIKE 'sqlite_%';
        """)
        tables = [row[0] for row in cursor.fetchall()]

        for table in tables:
            df = pd.read_sql(f"SELECT * FROM {table}", conn)
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)

            parquet_key = f"parquets_files/{table}.parquet"

            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=parquet_key,
                Body=parquet_buffer.getvalue()
            )

            print(f"Table '{table}' convertie en Parquet sur S3")

    finally:
        conn.close()
        os.remove(tmp_db_path)


s3_client = boto3.client('s3')

def convert_txt_to_parquet(path_to_file_key):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_file_key)
    content = obj['Body'].read()
    filename = path_to_file_key.split("/")[-1]

    df = pd.read_csv(io.BytesIO(content), sep=None, engine='python')

    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    parquet_key = f"parquets_files/{filename.rsplit('.',1)[0]}.parquet"

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=parquet_key,
        Body=parquet_buffer.getvalue()
    )

    print(f"Fichier Parquet chargé sur S3 : {parquet_key}")

def convert_shp_to_parquet(path_to_shp_key):
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_shp_key)
    content = obj['Body'].read()
    with tempfile.TemporaryDirectory() as tmpdir:
        shp_path = os.path.join(tmpdir, "file.shp")
        with open(shp_path, "wb") as f:
            f.write(content)
        gdf = gpd.read_file(shp_path)

        parquet_buffer = io.BytesIO()
        gdf.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        parquet_key = f"parquets_files/{os.path.basename(path_to_shp_key).rsplit('.',1)[0]}.parquet"
        s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=parquet_buffer.getvalue())

        print(f"SHP converti : {parquet_key}")

def convert_geojson_to_parquet(path_to_geojson_key):
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_geojson_key)
    content = io.BytesIO(obj['Body'].read())
    gdf = gpd.read_file(content)

    parquet_buffer = io.BytesIO()
    gdf.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    parquet_key = f"parquets_files/{os.path.basename(path_to_geojson_key).rsplit('.',1)[0]}.parquet"
    s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=parquet_buffer.getvalue())

    print(f"GeoJSON converti : {parquet_key}")

def convert_kml_to_parquet(path_to_kml_key):
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_kml_key)
    content = io.BytesIO(obj['Body'].read())
    gdf = gpd.read_file(content, driver="KML")

    parquet_buffer = io.BytesIO()
    gdf.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    parquet_key = f"parquets_files/{os.path.basename(path_to_kml_key).rsplit('.',1)[0]}.parquet"
    s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=parquet_buffer.getvalue())

    print(f"KML converti : {parquet_key}")

def convert_gpkg_to_parquet(path_to_gpkg_key):
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_gpkg_key)
    content = io.BytesIO(obj['Body'].read())
    with tempfile.NamedTemporaryFile(suffix=".gpkg") as tmpfile:
        tmpfile.write(content)
        tmpfile.flush()
        gdf = gpd.read_file(tmpfile.name)

    parquet_buffer = io.BytesIO()
    gdf.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    parquet_key = f"parquets_files/{os.path.basename(path_to_gpkg_key).rsplit('.',1)[0]}.parquet"
    s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=parquet_buffer.getvalue())

    print(f"GPKG converti : {parquet_key}")

def convert_dwg_to_parquet(path_to_dwg_key):
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_dwg_key)
    content = io.BytesIO(obj['Body'].read())
    doc = ezdxf.readfile(content)

    records = []
    for e in doc.modelspace():
        records.append(e.dxfattribs())
    df = pd.DataFrame(records)
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    parquet_key = f"parquets_files/{os.path.basename(path_to_dwg_key).rsplit('.',1)[0]}.parquet"
    s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=parquet_buffer.getvalue())

    print(f"DWG converti : {parquet_key}")


def convert_dxf_to_parquet(path_to_dxf_key):
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_dxf_key)
    content = io.BytesIO(obj['Body'].read())
    doc = ezdxf.readfile(content)
    records = []
    for e in doc.modelspace():
        records.append(e.dxfattribs())
    df = pd.DataFrame(records)
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    parquet_key = f"parquets_files/{os.path.basename(path_to_dxf_key).rsplit('.',1)[0]}.parquet"
    s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=parquet_buffer.getvalue())
    print(f"DXF converti : {parquet_key}")


def convert_json_to_parquet(path_to_json_key, lines=False):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_json_key)
    content = obj['Body'].read()
    df = pd.read_json(io.BytesIO(content), lines=lines)

    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    parquet_key = f"parquets_files/{path_to_json_key.split('/')[-1].rsplit('.',1)[0]}.parquet"
    s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=parquet_buffer.getvalue())
    print(f"JSON converti : {parquet_key}")

def convert_xml_to_parquet(path_to_xml_key, xpath=".//record"):
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_xml_key)
    content = obj['Body'].read()
    df = pd.read_xml(io.BytesIO(content), xpath=xpath)

    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    parquet_key = f"parquets_files/{path_to_xml_key.split('/')[-1].rsplit('.',1)[0]}.parquet"
    s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=parquet_buffer.getvalue())
    print(f"XML converti : {parquet_key}")





