
import pandas as pd
from dbfread import DBF
import tempfile
import os
from ingestion.s3.io import read_s3_object, write_s3_object
from transformation.transformat_files_to_parquet.parquet.writer import dataframe_to_parquet_bytes
from utils.config import S3_BUCKET

def convert_dbf_to_parquet(path_to_dbf_key, S3_BUCKET=S3_BUCKET):  
    dbf_content = read_s3_object(path_to_dbf_key)
    with tempfile.NamedTemporaryFile(suffix=".dbf", delete=False) as tmp_dbf:
        tmp_dbf.write(dbf_content)
        tmp_dbf_path = tmp_dbf.name

    try:
        table = DBF(tmp_dbf_path, encoding='utf-8')
        df = pd.DataFrame(iter(table))
        parquet_buffer = dataframe_to_parquet_bytes(df)
        
        dbf_filename = os.path.basename(path_to_dbf_key).rsplit('.', 1)[0]
        parquet_key = f"parquets_files/{dbf_filename}.parquet" 
        write_s3_object(parquet_key, parquet_buffer)
        print(f"Fichier Parquet chargé sur S3 : {parquet_key}")
        
    finally:
        os.remove(tmp_dbf_path)

