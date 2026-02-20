from ingestion.s3.io import read_s3_object
import sqlite3
import tempfile
import os
import pandas as pd
from transformation.transformat_files_to_parquet.parquet.writer import dataframe_to_parquet_bytes
from ingestion.s3.io import write_s3_object
from utils.config import S3_BUCKET

def convert_db_to_parquet_all_tables(path_to_db_key, S3_BUCKET=S3_BUCKET):
    db_content = read_s3_object(path_to_db_key)

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

            parquet_buffer = dataframe_to_parquet_bytes(df)

            parquet_key = f"parquets_files/{table}.parquet"

            write_s3_object(parquet_key, parquet_buffer)

            print(f"Table {table} → {parquet_key}")

    finally:
        conn.close()
        os.remove(tmp_db_path)
