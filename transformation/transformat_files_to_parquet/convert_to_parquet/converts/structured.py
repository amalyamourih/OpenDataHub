from abc import ABC
import pandas as pd
from .base import BaseConverter
from ingestion.s3.io import write_s3_object
from transformation.transformat_files_to_parquet.parquet.writer import dataframe_to_parquet_bytes



class StructuredFileConverter(BaseConverter, ABC):

    def _export(self, df: pd.DataFrame, s3_key: str):
        parquet_bytes = dataframe_to_parquet_bytes(df)
        parquet_key = (
            f"parquets_files/"
            f"{s3_key.split('/')[-1].rsplit('.', 1)[0]}.parquet"
        )
        write_s3_object(parquet_key, parquet_bytes)
        print(f"{self.__class__.__name__} converti : {parquet_key}")
