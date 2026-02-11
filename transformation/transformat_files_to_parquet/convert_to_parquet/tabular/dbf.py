import io
import pandas as pd
from dbfread import DBF
from transformation.transformat_files_to_parquet.convert_to_parquet.converts.base import BaseConverter
from ingestion.s3.io import read_s3_object, write_s3_object
from transformation.transformat_files_to_parquet.parquet.writer import dataframe_to_parquet_bytes

class DBFConverter(BaseConverter):

    def convert(self, s3_key: str):
        content = read_s3_object(s3_key)

        table = DBF(
            io.BytesIO(content),
            load=True,
            char_decode_errors="ignore"
        )

        df = pd.DataFrame(iter(table))

        parquet_bytes = dataframe_to_parquet_bytes(df)
        parquet_key = f"parquets_files/{s3_key.split('/')[-1].rsplit('.',1)[0]}.parquet"

        write_s3_object(parquet_key, parquet_bytes)
        print(f"DBF → Parquet : {parquet_key}")
