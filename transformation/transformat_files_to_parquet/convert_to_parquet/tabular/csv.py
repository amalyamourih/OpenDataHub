import io
import pandas as pd
from ingestion.s3.io import read_s3_object, write_s3_object
from transformation.transformat_files_to_parquet.parquet.writer import dataframe_to_parquet_bytes
from utils.config import S3_BUCKET

def convert_csv_to_parquet(path_to_csv_key, S3_BUCKET=S3_BUCKET):
    csv_content = read_s3_object(path_to_csv_key)
    df = pd.read_csv(io.BytesIO(csv_content))
    parquet_buffer = dataframe_to_parquet_bytes(df)
 
    csv_filename = path_to_csv_key.split("/")[-1].replace(".csv", ".parquet")
    parquet_key = f"parquets_files/{csv_filename}"
    write_s3_object(parquet_key, parquet_buffer)


    print(f"Fichier Parquet chargé sur S3")
