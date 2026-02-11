import io
import pandas as pd
from transformation.transformat_files_to_parquet.convert_to_parquet.converts.base import BaseConverter
from ingestion.s3.io import read_s3_object, write_s3_object
from transformation.transformat_files_to_parquet.parquet.writer import dataframe_to_parquet_bytes

class ExcelConverter(BaseConverter):

    def __init__(self, engine=None):
        self.engine = engine

    def convert(self, s3_key: str):
        content = read_s3_object(s3_key)
        excel = pd.ExcelFile(io.BytesIO(content), engine=self.engine)

        for sheet in excel.sheet_names:
            df = pd.read_excel(excel, sheet_name=sheet, engine=self.engine)
            parquet_bytes = dataframe_to_parquet_bytes(df)

            parquet_key = f"parquets_files/{sheet}.parquet"
            write_s3_object(parquet_key, parquet_bytes)

            print(f"Excel [{sheet}] → Parquet")
