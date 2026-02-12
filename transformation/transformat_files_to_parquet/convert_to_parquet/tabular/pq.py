from ingestion.s3.io import read_s3_object, write_s3_object
from utils.config import S3_BUCKET
import os

def convert_pq_to_parquet(path_to_pq_key, S3_BUCKET=S3_BUCKET):
    content = read_s3_object(path_to_pq_key)
    parquet_key = f"parquets_files/{os.path.basename(path_to_pq_key).rsplit('.', 1)[0]}.parquet"
    write_s3_object(parquet_key, content)
    
    print(f"PQ copié : {parquet_key}")