import io
import pandas as pd
from ingestion.s3.io import read_s3_object, write_s3_object
from transformation.transformat_files_to_parquet.parquet.writer import dataframe_to_parquet_bytes
from utils.config import S3_BUCKET

def convert_xls_to_parquet(path_to_xls_key, S3_BUCKET=S3_BUCKET):
    xls_content = read_s3_object(path_to_xls_key)
    excel_file = pd.ExcelFile(io.BytesIO(xls_content), engine="xlrd")  # moteur pour .xls

    for sheet_name in excel_file.sheet_names:
        df = pd.read_excel(excel_file, sheet_name=sheet_name, engine="xlrd")
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str)


        parquet_buffer = dataframe_to_parquet_bytes(df)
        parquet_key = f"parquets_files/{sheet_name}.parquet"
        write_s3_object(parquet_key, parquet_buffer)

        print(f"Feuille '{sheet_name}' convertie en Parquet")
