from ingestion.s3.io import read_s3_object
import sqlite3
import tempfile
import os
from converts.database import SQLiteToParquet

class SQLConverter(SQLiteToParquet):

    def convert(self, s3_key: str):
        sql_content = read_s3_object(s3_key).decode("utf-8")

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            tmp_db_path = tmp.name

        try:
            conn = sqlite3.connect(tmp_db_path)
            cursor = conn.cursor()

            cursor.executescript(sql_content)
            conn.commit()

            self._export_all_tables(conn)

        finally:
            conn.close()
            os.remove(tmp_db_path)
