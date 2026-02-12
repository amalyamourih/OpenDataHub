import io 
import pandas as pd
from ingestion.s3.io import read_s3_object, write_s3_object
from transformation.transformat_files_to_parquet.parquet.writer import dataframe_to_parquet_bytes
from utils.config import S3_BUCKET

def convert_xml_to_parquet(path_to_xml_key, xpath=".//record", S3_BUCKET=S3_BUCKET):
    content = read_s3_object(path_to_xml_key)
    df = pd.read_xml(io.BytesIO(content), xpath=xpath)

    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)

    parquet_buffer = dataframe_to_parquet_bytes(df)

    parquet_key = f"parquets_files/{path_to_xml_key.split('/')[-1].rsplit('.',1)[0]}.parquet"
    write_s3_object(parquet_key, parquet_buffer.read())
    print(f"XML converti : {parquet_key}")
