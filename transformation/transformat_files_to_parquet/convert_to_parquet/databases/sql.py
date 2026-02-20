from ingestion.s3.io import read_s3_object , write_s3_object
from transformation.transformat_files_to_parquet.parquet.writer import dataframe_to_parquet_bytes
import sqlite3
import tempfile
import os
import pandas as pd
from utils.config import S3_BUCKET

def convert_sql_to_parquet(path_to_sql_key, S3_BUCKET=S3_BUCKET):
    sql_content = read_s3_object(path_to_sql_key)
    sql_content = sql_content.decode('utf-8')

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
            parquet_buffer = dataframe_to_parquet_bytes(df)

            parquet_key = f"parquets_files/{table}.parquet"
            write_s3_object(parquet_key, parquet_buffer)

            print(f"Table '{table}' convertie en Parquet sur S3")

    finally:
        conn.close()
        os.remove(tmp_db_path)
