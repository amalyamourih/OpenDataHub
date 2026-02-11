from ingestion.s3.io import read_s3_object
import sqlite3
import tempfile
import os
from converts.database import SQLiteToParquet

class DBConverter(SQLiteToParquet):

    def convert(self, s3_key: str):
        db_content = read_s3_object(s3_key)

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            tmp.write(db_content)
            tmp_db_path = tmp.name

        try:
            conn = sqlite3.connect(tmp_db_path)
            self._export_all_tables(conn)

        finally:
            conn.close()
            os.remove(tmp_db_path)
