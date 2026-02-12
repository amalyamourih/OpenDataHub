from ingestion.s3.io import read_s3_object, write_s3_object
from utils.config import S3_BUCKET
import os


def convert_parquet_to_parquet(path_to_parquet_key, S3_BUCKET=S3_BUCKET):
    content = read_s3_object(path_to_parquet_key)
    
    parquet_key = f"parquets_files/{os.path.basename(path_to_parquet_key)}"
    write_s3_object(parquet_key, content)
    
    print(f"Parquet copié : {parquet_key}")