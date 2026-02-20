import pandas as pd 
import boto3
from utils.config import S3_BUCKET
import io
import os
import sqlite3
import tempfile
import geopandas as gpd
import ezdxf
import zipfile
import tarfile
import gzip
import lzma
import bz2
import json
import rasterio
import numpy as np
from dbfread import DBF
import fitz  
from docx import Document
from odf.opendocument import load as load_odf
from odf.text import P
import py7zr
import rarfile


#TODO : "conversion vers pdf, dock, opt"
def convert_csv_to_parquet(path_to_csv_key, S3_BUCKET=S3_BUCKET):
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

def convert_tsv_to_parquet(path_to_tsv_key, S3_BUCKET=S3_BUCKET):
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


def convert_db_to_parquet_all_tables(path_to_db_key, S3_BUCKET=S3_BUCKET):
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

def convert_xlsx_to_parquet(path_to_xlsx_key, S3_BUCKET=S3_BUCKET):
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

def convert_xls_to_parquet(path_to_xls_key, S3_BUCKET=S3_BUCKET):
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

   
def convert_sql_to_parquet(path_to_sql_key, S3_BUCKET=S3_BUCKET):
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

def convert_txt_to_parquet(path_to_file_key, S3_BUCKET=S3_BUCKET):
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

def convert_shp_to_parquet(path_to_shp_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    extensions = ['.shp', '.shx', '.dbf', '.prj', '.cpg']
    base_key = path_to_shp_key.rsplit('.', 1)[0]
    
    with tempfile.TemporaryDirectory() as tmpdir:
        base_local = os.path.join(tmpdir, "file")
        
        for ext in extensions:
            s3_key = base_key + ext
            local_path = base_local + ext
            try:
                obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
                with open(local_path, 'wb') as f:
                    f.write(obj['Body'].read())
            except s3_client.exceptions.NoSuchKey:
                if ext in ['.shp', '.shx', '.dbf']:
                    raise
        
        # Lire le shapefile complet
        gdf = gpd.read_file(base_local + ".shp")
        
        parquet_buffer = io.BytesIO()
        gdf.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        
        parquet_key = f"parquets_files/{os.path.basename(path_to_shp_key).rsplit('.', 1)[0]}.parquet"
        s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=parquet_buffer.getvalue())
        print(f"SHP converti : {parquet_key}")

def convert_shz_to_parquet(path_to_shz_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_shz_key)
    content = obj['Body'].read()
    with tempfile.TemporaryDirectory() as tmpdir:
        shz_path = os.path.join(tmpdir, "file.shz")
        with open(shz_path, "wb") as f:
            f.write(content)
        gdf = gpd.read_file(shz_path)

        parquet_buffer = io.BytesIO()
        gdf.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        parquet_key = f"parquets_files/{os.path.basename(path_to_shz_key).rsplit('.',1)[0]}.parquet"
        s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=parquet_buffer.getvalue())

        print(f"SHZ converti : {parquet_key}")

def convert_geojson_to_parquet(path_to_geojson_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_geojson_key)
    content = io.BytesIO(obj['Body'].read())
    gdf = gpd.read_file(content)

    parquet_buffer = io.BytesIO()
    gdf.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    parquet_key = f"parquets_files/{os.path.basename(path_to_geojson_key).rsplit('.',1)[0]}.parquet"
    s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=parquet_buffer.getvalue())

    print(f"GeoJSON converti : {parquet_key}")

def convert_kml_to_parquet(path_to_kml_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_kml_key)
    content = io.BytesIO(obj['Body'].read())
    gdf = gpd.read_file(content, driver="KML")

    parquet_buffer = io.BytesIO()
    gdf.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    parquet_key = f"parquets_files/{os.path.basename(path_to_kml_key).rsplit('.',1)[0]}.parquet"
    s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=parquet_buffer.getvalue())

    print(f"KML converti : {parquet_key}")

def convert_kmz_to_parquet(path_to_kmz_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_kmz_key)
    content = io.BytesIO(obj['Body'].read())
    gdf = gpd.read_file(content, driver="KML")

    parquet_buffer = io.BytesIO()
    gdf.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    parquet_key = f"parquets_files/{os.path.basename(path_to_kmz_key).rsplit('.',1)[0]}.parquet"
    s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=parquet_buffer.getvalue())

    print(f"KMZ converti : {parquet_key}")

def convert_gpkg_to_parquet(path_to_gpkg_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_gpkg_key)
    content = obj['Body'].read() 
    with tempfile.TemporaryDirectory() as tmpdir:
        gpkg_path = os.path.join(tmpdir, "file.gpkg")
        with open(gpkg_path, 'wb') as f:
            f.write(content)
        
        gdf = gpd.read_file(gpkg_path)

    parquet_buffer = io.BytesIO()
    gdf.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    parquet_key = f"parquets_files/{os.path.basename(path_to_gpkg_key).rsplit('.', 1)[0]}.parquet"
    s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=parquet_buffer.getvalue())
    print(f"GPKG converti : {parquet_key}")

def convert_dwg_to_parquet(path_to_dwg_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_dwg_key)
    content = obj['Body'].read()  # ✅ bytes, pas BytesIO
    
    with tempfile.TemporaryDirectory() as tmpdir:
        dwg_path = os.path.join(tmpdir, "file.dwg")
        with open(dwg_path, 'wb') as f:
            f.write(content)
        
        doc = ezdxf.readfile(dwg_path)  

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

        parquet_key = f"parquets_files/{os.path.basename(path_to_dwg_key).rsplit('.', 1)[0]}.parquet"
        s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=parquet_buffer.getvalue())
        print(f"DWG converti : {parquet_key}")


def convert_dxf_to_parquet(path_to_dxf_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_dxf_key)
    content = obj['Body'].read()      
    with tempfile.TemporaryDirectory() as tmpdir:
        dxf_path = os.path.join(tmpdir, "file.dxf")
        with open(dxf_path, 'wb') as f:
            f.write(content)
        
        doc = ezdxf.readfile(dxf_path)  
        
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

        parquet_key = f"parquets_files/{os.path.basename(path_to_dxf_key).rsplit('.', 1)[0]}.parquet"
        s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=parquet_buffer.getvalue())
        print(f"DXF converti : {parquet_key}")


def convert_json_to_parquet(path_to_json_key, lines=False, S3_BUCKET=S3_BUCKET):
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

def convert_xml_to_parquet(path_to_xml_key, xpath=".//record", S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
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


def convert_ods_to_parquet(path_to_ods_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_ods_key)
    ods_content = obj['Body'].read()
    excel_file = pd.ExcelFile(io.BytesIO(ods_content), engine="odf")
    
    for sheet_name in excel_file.sheet_names:
        df = pd.read_excel(excel_file, sheet_name=sheet_name, engine="odf")
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str)
        
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        parquet_key = f"parquets_files/{sheet_name}.parquet"
        s3_client.put_object(Bucket=S3_BUCKET,Key=parquet_key,Body=parquet_buffer.getvalue())
        print(f"Feuille '{sheet_name}' convertie en Parquet")
        
    

def convert_dbf_to_parquet(path_to_dbf_key, S3_BUCKET=S3_BUCKET):  
    s3_client = boto3.client('s3') 
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_dbf_key)
    dbf_content = obj['Body'].read()
    with tempfile.NamedTemporaryFile(suffix=".dbf", delete=False) as tmp_dbf:
        tmp_dbf.write(dbf_content)
        tmp_dbf_path = tmp_dbf.name

    try:
        table = DBF(tmp_dbf_path, encoding='utf-8')
        df = pd.DataFrame(iter(table))
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str)
        
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        
        dbf_filename = os.path.basename(path_to_dbf_key).rsplit('.', 1)[0]
        parquet_key = f"parquets_files/{dbf_filename}.parquet" 
        s3_client.put_object(Bucket=S3_BUCKET,Key=parquet_key,Body=parquet_buffer.getvalue())
        print(f"DBF converti : {parquet_key}")
        
    finally:
        os.remove(tmp_dbf_path)

def convert_tiff_to_parquet(path_to_tiff_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_tiff_key)
    content = obj['Body'].read()
    
    with tempfile.NamedTemporaryFile(suffix=".tiff", delete=False) as tmp:
        tmp.write(content)
        tmp_path = tmp.name
    
    try:
        with rasterio.open(tmp_path) as src:
            # Lire les métadonnées
            meta = {
                'crs': str(src.crs),
                'transform': str(src.transform),
                'width': src.width,
                'height': src.height,
                'count': src.count,
                'dtype': str(src.dtypes[0])
            }
            
            # Lire les données raster et les aplatir
            records = []
            for band_idx in range(1, src.count + 1):
                band_data = src.read(band_idx)
                for row in range(src.height):
                    for col in range(src.width):
                        x, y = src.xy(row, col)
                        records.append({
                            'band': band_idx,
                            'row': row,
                            'col': col,
                            'x': x,
                            'y': y,
                            'value': float(band_data[row, col])
                        })
            
            df = pd.DataFrame(records)
            # Ajouter les métadonnées comme colonnes constantes
            for key, val in meta.items():
                df[f'meta_{key}'] = str(val)
        
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        
        parquet_key = f"parquets_files/{os.path.basename(path_to_tiff_key).rsplit('.', 1)[0]}.parquet"
        s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=parquet_buffer.getvalue())
        
        print(f"TIFF converti : {parquet_key}")
        
    finally:
        os.remove(tmp_path)


def convert_tif_to_parquet(path_to_tif_key, S3_BUCKET=S3_BUCKET):

    convert_tiff_to_parquet(path_to_tif_key, S3_BUCKET)


def convert_jp2_to_parquet(path_to_jp2_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_jp2_key)
    content = obj['Body'].read()
    
    with tempfile.NamedTemporaryFile(suffix=".jp2", delete=False) as tmp:
        tmp.write(content)
        tmp_path = tmp.name
    
    try:
        with rasterio.open(tmp_path) as src:
            meta = {
                'crs': str(src.crs),
                'transform': str(src.transform),
                'width': src.width,
                'height': src.height,
                'count': src.count
            }
            
            records = []
            for band_idx in range(1, src.count + 1):
                band_data = src.read(band_idx)
                for row in range(src.height):
                    for col in range(src.width):
                        x, y = src.xy(row, col)
                        records.append({
                            'band': band_idx,
                            'row': row,
                            'col': col,
                            'x': x,
                            'y': y,
                            'value': float(band_data[row, col])
                        })
            
            df = pd.DataFrame(records)
            for key, val in meta.items():
                df[f'meta_{key}'] = str(val)
        
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        
        parquet_key = f"parquets_files/{os.path.basename(path_to_jp2_key).rsplit('.', 1)[0]}.parquet"
        s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=parquet_buffer.getvalue())
        
        print(f"JP2 converti : {parquet_key}")
        
    finally:
        os.remove(tmp_path)


def convert_ecw_to_parquet(path_to_ecw_key, S3_BUCKET=S3_BUCKET):
    """Convertit un ECW en Parquet (nécessite GDAL avec support ECW)"""
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_ecw_key)
    content = obj['Body'].read()
    
    with tempfile.NamedTemporaryFile(suffix=".ecw", delete=False) as tmp:
        tmp.write(content)
        tmp_path = tmp.name
    
    try:
        with rasterio.open(tmp_path) as src:
            meta = {
                'crs': str(src.crs),
                'transform': str(src.transform),
                'width': src.width,
                'height': src.height,
                'count': src.count
            }
            
            records = []
            for band_idx in range(1, src.count + 1):
                band_data = src.read(band_idx)
                for row in range(src.height):
                    for col in range(src.width):
                        x, y = src.xy(row, col)
                        records.append({
                            'band': band_idx,
                            'row': row,
                            'col': col,
                            'x': x,
                            'y': y,
                            'value': float(band_data[row, col])
                        })
            
            df = pd.DataFrame(records)
            for key, val in meta.items():
                df[f'meta_{key}'] = str(val)
        
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        
        parquet_key = f"parquets_files/{os.path.basename(path_to_ecw_key).rsplit('.', 1)[0]}.parquet"
        s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=parquet_buffer.getvalue())
        
        print(f"ECW converti : {parquet_key}")
        
    finally:
        os.remove(tmp_path)



def _convert_by_extension(file_key, ext, S3_BUCKET=S3_BUCKET):
    """Route vers la fonction de conversion appropriée selon l'extension"""
    
    converters = {
        # Tabular
        'csv': convert_csv_to_parquet,
        'tsv': convert_tsv_to_parquet,
        'xlsx': convert_xlsx_to_parquet,
        'xls': convert_xls_to_parquet,
        'ods': convert_ods_to_parquet,
        'txt': convert_txt_to_parquet,
        'dbf': convert_dbf_to_parquet,
        'parquet': convert_parquet_to_parquet,
        'pq': convert_pq_to_parquet,
        
        # Geospatial Vector
        'shp': convert_shp_to_parquet,
        'shz': convert_shz_to_parquet,
        'geojson': convert_geojson_to_parquet,
        'kml': convert_kml_to_parquet,
        'kmz': convert_kmz_to_parquet,
        'gpkg': convert_gpkg_to_parquet,
        'dxf': convert_dxf_to_parquet,
        
        # Geospatial Raster
        'tiff': convert_tiff_to_parquet,
        'tif': convert_tif_to_parquet,
        'jp2': convert_jp2_to_parquet,
        'ecw': convert_ecw_to_parquet,
        
        # Databases
        'db': convert_db_to_parquet_all_tables,
        'sql': convert_sql_to_parquet,
        
        # Documents
        'pdf': convert_pdf_to_parquet,
        'docx': convert_docx_to_parquet,
        'odt': convert_odt_to_parquet,
        
        # Data formats
        'json': lambda key, bucket=S3_BUCKET: convert_json_to_parquet(key, lines=False, S3_BUCKET=bucket),
        'jsonl': lambda key, bucket=S3_BUCKET: convert_json_to_parquet(key, lines=True, S3_BUCKET=bucket),
        'xml': convert_xml_to_parquet,
        
        # Archives (récursif)
        'zip': convert_zip_to_parquet,
        'tar': convert_tar_to_parquet,
        'tgz': convert_tgz_to_parquet,
        'gz': convert_gz_to_parquet,
        'xz': convert_xz_to_parquet,
        'bz2': convert_bz2_to_parquet,
        '7z': convert_7z_to_parquet,
        'rar': convert_rar_to_parquet,
    }
    
    ext_lower = ext.lower()
    
    if ext_lower in converters:
        try:
            converters[ext_lower](file_key, S3_BUCKET)
            print(f" Converti: {file_key}")
        except Exception as e:
            print(f" Erreur pour {file_key}: {e}")
    else:
        print(f" Extension non supportée: .{ext} ({file_key})")

def convert_zip_to_parquet(path_to_zip_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_zip_key)
    content = obj['Body'].read()
    
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "archive.zip")
        with open(zip_path, 'wb') as f:
            f.write(content)
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(tmpdir)
        
        # Parcourir les fichiers extraits
        for root, dirs, files in os.walk(tmpdir):
            for file in files:
                if file == "archive.zip":
                    continue
                file_path = os.path.join(root, file)
                ext = file.rsplit('.', 1)[-1].lower() if '.' in file else ''
                
                # Upload le fichier extrait sur S3 puis convertir
                with open(file_path, 'rb') as f:
                    file_content = f.read()
                
                temp_key = f"temp_extracted/{file}"
                s3_client.put_object(Bucket=S3_BUCKET, Key=temp_key, Body=file_content)
                
                # Appeler la fonction de conversion appropriée
                _convert_by_extension(temp_key, ext, S3_BUCKET)
                
                # Nettoyer le fichier temporaire
                s3_client.delete_object(Bucket=S3_BUCKET, Key=temp_key)
    
    print(f"ZIP extrait et converti : {path_to_zip_key}")


def convert_tar_to_parquet(path_to_tar_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_tar_key)
    content = obj['Body'].read()
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tar_path = os.path.join(tmpdir, "archive.tar")
        with open(tar_path, 'wb') as f:
            f.write(content)
        
        with tarfile.open(tar_path, 'r') as tar_ref:
            tar_ref.extractall(tmpdir)
        
        for root, dirs, files in os.walk(tmpdir):
            for file in files:
                if file == "archive.tar":
                    continue
                file_path = os.path.join(root, file)
                ext = file.rsplit('.', 1)[-1].lower() if '.' in file else ''
                
                with open(file_path, 'rb') as f:
                    file_content = f.read()
                
                temp_key = f"temp_extracted/{file}"
                s3_client.put_object(Bucket=S3_BUCKET, Key=temp_key, Body=file_content)
                _convert_by_extension(temp_key, ext, S3_BUCKET)
                s3_client.delete_object(Bucket=S3_BUCKET, Key=temp_key)
    
    print(f"TAR extrait et converti : {path_to_tar_key}")


def convert_tgz_to_parquet(path_to_tgz_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_tgz_key)
    content = obj['Body'].read()
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tgz_path = os.path.join(tmpdir, "archive.tgz")
        with open(tgz_path, 'wb') as f:
            f.write(content)
        
        with tarfile.open(tgz_path, 'r:gz') as tar_ref:
            tar_ref.extractall(tmpdir)
        
        for root, dirs, files in os.walk(tmpdir):
            for file in files:
                if file == "archive.tgz":
                    continue
                file_path = os.path.join(root, file)
                ext = file.rsplit('.', 1)[-1].lower() if '.' in file else ''
                
                with open(file_path, 'rb') as f:
                    file_content = f.read()
                
                temp_key = f"temp_extracted/{file}"
                s3_client.put_object(Bucket=S3_BUCKET, Key=temp_key, Body=file_content)
                _convert_by_extension(temp_key, ext, S3_BUCKET)
                s3_client.delete_object(Bucket=S3_BUCKET, Key=temp_key)
    
    print(f"TGZ extrait et converti : {path_to_tgz_key}")


def convert_gz_to_parquet(path_to_gz_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_gz_key)
    content = obj['Body'].read()
    
    # Décompresser
    decompressed = gzip.decompress(content)
    
    # Déterminer l'extension du fichier décompressé
    filename = os.path.basename(path_to_gz_key)
    if filename.endswith('.gz'):
        inner_filename = filename[:-3]
    else:
        inner_filename = filename + ".decompressed"
    
    ext = inner_filename.rsplit('.', 1)[-1].lower() if '.' in inner_filename else 'txt'
    
    temp_key = f"temp_extracted/{inner_filename}"
    s3_client.put_object(Bucket=S3_BUCKET, Key=temp_key, Body=decompressed)
    _convert_by_extension(temp_key, ext, S3_BUCKET)
    s3_client.delete_object(Bucket=S3_BUCKET, Key=temp_key)
    
    print(f"GZ décompressé et converti : {path_to_gz_key}")


def convert_xz_to_parquet(path_to_xz_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_xz_key)
    content = obj['Body'].read()
    
    decompressed = lzma.decompress(content)
    
    filename = os.path.basename(path_to_xz_key)
    if filename.endswith('.xz'):
        inner_filename = filename[:-3]
    else:
        inner_filename = filename + ".decompressed"
    
    ext = inner_filename.rsplit('.', 1)[-1].lower() if '.' in inner_filename else 'txt'
    
    temp_key = f"temp_extracted/{inner_filename}"
    s3_client.put_object(Bucket=S3_BUCKET, Key=temp_key, Body=decompressed)
    _convert_by_extension(temp_key, ext, S3_BUCKET)
    s3_client.delete_object(Bucket=S3_BUCKET, Key=temp_key)
    
    print(f"XZ décompressé et converti : {path_to_xz_key}")


def convert_bz2_to_parquet(path_to_bz2_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_bz2_key)
    content = obj['Body'].read()
    
    decompressed = bz2.decompress(content)
    
    filename = os.path.basename(path_to_bz2_key)
    if filename.endswith('.bz2'):
        inner_filename = filename[:-4]
    else:
        inner_filename = filename + ".decompressed"
    
    ext = inner_filename.rsplit('.', 1)[-1].lower() if '.' in inner_filename else 'txt'
    
    temp_key = f"temp_extracted/{inner_filename}"
    s3_client.put_object(Bucket=S3_BUCKET, Key=temp_key, Body=decompressed)
    _convert_by_extension(temp_key, ext, S3_BUCKET)
    s3_client.delete_object(Bucket=S3_BUCKET, Key=temp_key)
    
    print(f"BZ2 décompressé et converti : {path_to_bz2_key}")


def convert_7z_to_parquet(path_to_7z_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_7z_key)
    content = obj['Body'].read()
    
    with tempfile.TemporaryDirectory() as tmpdir:
        archive_path = os.path.join(tmpdir, "archive.7z")
        with open(archive_path, 'wb') as f:
            f.write(content)
        
        with py7zr.SevenZipFile(archive_path, mode='r') as z:
            z.extractall(path=tmpdir)
        
        for root, dirs, files in os.walk(tmpdir):
            for file in files:
                if file == "archive.7z":
                    continue
                file_path = os.path.join(root, file)
                ext = file.rsplit('.', 1)[-1].lower() if '.' in file else ''
                
                with open(file_path, 'rb') as f:
                    file_content = f.read()
                
                temp_key = f"temp_extracted/{file}"
                s3_client.put_object(Bucket=S3_BUCKET, Key=temp_key, Body=file_content)
                _convert_by_extension(temp_key, ext, S3_BUCKET)
                s3_client.delete_object(Bucket=S3_BUCKET, Key=temp_key)
    
    print(f"7Z extrait et converti : {path_to_7z_key}")


def convert_rar_to_parquet(path_to_rar_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_rar_key)
    content = obj['Body'].read()
    
    with tempfile.TemporaryDirectory() as tmpdir:
        archive_path = os.path.join(tmpdir, "archive.rar")
        with open(archive_path, 'wb') as f:
            f.write(content)
        
        with rarfile.RarFile(archive_path) as rf:
            rf.extractall(tmpdir)
        
        for root, dirs, files in os.walk(tmpdir):
            for file in files:
                if file == "archive.rar":
                    continue
                file_path = os.path.join(root, file)
                ext = file.rsplit('.', 1)[-1].lower() if '.' in file else ''
                
                with open(file_path, 'rb') as f:
                    file_content = f.read()
                
                temp_key = f"temp_extracted/{file}"
                s3_client.put_object(Bucket=S3_BUCKET, Key=temp_key, Body=file_content)
                _convert_by_extension(temp_key, ext, S3_BUCKET)
                s3_client.delete_object(Bucket=S3_BUCKET, Key=temp_key)
    
    print(f"RAR extrait et converti : {path_to_rar_key}")



def convert_pdf_to_parquet(path_to_pdf_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_pdf_key)
    content = obj['Body'].read()
    
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(content)
        tmp_path = tmp.name
    
    try:
        doc = fitz.open(tmp_path)
        records = []
        
        for page_num in range(len(doc)):
            page = doc[page_num]
            text = page.get_text()
            records.append({
                'page_number': page_num + 1,
                'text': text,
                'char_count': len(text),
                'word_count': len(text.split())
            })
        
        doc.close()
        df = pd.DataFrame(records)
        
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        
        parquet_key = f"parquets_files/{os.path.basename(path_to_pdf_key).rsplit('.', 1)[0]}.parquet"
        s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=parquet_buffer.getvalue())
        
        print(f"PDF converti : {parquet_key}")
        
    finally:
        os.remove(tmp_path)


def convert_docx_to_parquet(path_to_docx_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_docx_key)
    content = obj['Body'].read()
    
    with tempfile.NamedTemporaryFile(suffix=".docx", delete=False) as tmp:
        tmp.write(content)
        tmp_path = tmp.name
    
    try:
        doc = Document(tmp_path)
        records = []
        
        for para_num, para in enumerate(doc.paragraphs):
            if para.text.strip():  # Ignorer les paragraphes vides
                records.append({
                    'paragraph_number': para_num + 1,
                    'text': para.text,
                    'style': para.style.name if para.style else None,
                    'char_count': len(para.text),
                    'word_count': len(para.text.split())
                })
        
        df = pd.DataFrame(records)
        
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        
        parquet_key = f"parquets_files/{os.path.basename(path_to_docx_key).rsplit('.', 1)[0]}.parquet"
        s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=parquet_buffer.getvalue())
        
        print(f"DOCX converti : {parquet_key}")
        
    finally:
        os.remove(tmp_path)


def convert_odt_to_parquet(path_to_odt_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_odt_key)
    content = obj['Body'].read()
    
    with tempfile.NamedTemporaryFile(suffix=".odt", delete=False) as tmp:
        tmp.write(content)
        tmp_path = tmp.name
    
    try:
        doc = load_odf(tmp_path)
        paragraphs = doc.getElementsByType(P)
        
        records = []
        for para_num, para in enumerate(paragraphs):
            text = "".join([str(node) for node in para.childNodes if node.nodeType == node.TEXT_NODE])
            if text.strip():
                records.append({
                    'paragraph_number': para_num + 1,
                    'text': text,
                    'char_count': len(text),
                    'word_count': len(text.split())
                })
        
        df = pd.DataFrame(records)
        
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        
        parquet_key = f"parquets_files/{os.path.basename(path_to_odt_key).rsplit('.', 1)[0]}.parquet"
        s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=parquet_buffer.getvalue())
        
        print(f"ODT converti : {parquet_key}")
        
    finally:
        os.remove(tmp_path)



def convert_pq_to_parquet(path_to_pq_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_pq_key)
    content = obj['Body'].read()
    
    parquet_key = f"parquets_files/{os.path.basename(path_to_pq_key).rsplit('.', 1)[0]}.parquet"
    s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=content)
    
    print(f"PQ copié : {parquet_key}")


def convert_parquet_to_parquet(path_to_parquet_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=path_to_parquet_key)
    content = obj['Body'].read()
    
    parquet_key = f"parquets_files/{os.path.basename(path_to_parquet_key)}"
    s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=content)
    
    print(f"Parquet copié : {parquet_key}")
