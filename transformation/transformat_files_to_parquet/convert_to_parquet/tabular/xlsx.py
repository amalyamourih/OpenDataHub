import io
import pandas as pd
from ingestion.s3.io import read_s3_object, write_s3_object
from transformation.transformat_files_to_parquet.parquet.writer import dataframe_to_parquet_bytes
from utils.config import S3_BUCKET

def convert_xlsx_to_parquet(path_to_xlsx_key, S3_BUCKET=S3_BUCKET):
    xlsx_content = read_s3_object(path_to_xlsx_key)

    excel_file = pd.ExcelFile(io.BytesIO(xlsx_content))

    for sheet_name in excel_file.sheet_names:
        df = pd.read_excel(excel_file, sheet_name=sheet_name)

        parquet_buffer = dataframe_to_parquet_bytes(df)

        parquet_key = f"parquets_files/{sheet_name}.parquet"

        write_s3_object(parquet_key, parquet_buffer)

        print(f"Feuille '{sheet_name}' convertie en Parquet")
