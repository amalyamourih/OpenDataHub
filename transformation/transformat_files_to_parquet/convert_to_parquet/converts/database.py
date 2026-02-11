import io
import pandas as pd
import sqlite3
import tempfile
from abc import ABC
from transformation.transformat_files_to_parquet.convert_to_parquet.converts.base import BaseConverter
from ingestion.s3.io import read_s3_object, write_s3_object
from transformation.transformat_files_to_parquet.parquet.writer import dataframe_to_parquet_bytes

class SQLiteToParquet(BaseConverter, ABC):

    def _export_all_tables(self, conn: sqlite3.Connection):
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type='table'
              AND name NOT LIKE 'sqlite_%';
        """)

        tables = [row[0] for row in cursor.fetchall()]

        for table in tables:
            df = pd.read_sql(f"SELECT * FROM {table}", conn)

            parquet_bytes = dataframe_to_parquet_bytes(df)
            parquet_key = f"parquets_files/{table}.parquet"

            write_s3_object(parquet_key, parquet_bytes)
            print(f"Table '{table}' → {parquet_key}")
