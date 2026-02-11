import io
import pandas as pd
from transformation.transformat_files_to_parquet.convert_to_parquet.converts.base import BaseConverter
from ingestion.s3.io import read_s3_object, write_s3_object
from transformation.transformat_files_to_parquet.parquet.writer import dataframe_to_parquet_bytes

class TXTConverter(BaseConverter):

    def convert(self, s3_key: str):
        content = read_s3_object(s3_key)

        df = pd.read_csv(
            io.BytesIO(content),
            sep=None,
            engine="python"
        )

        parquet_bytes = dataframe_to_parquet_bytes(df)

        parquet_key = (
            f"parquets_files/"
            f"{s3_key.split('/')[-1].rsplit('.', 1)[0]}.parquet"
        )

        write_s3_object(parquet_key, parquet_bytes)
        print(f"Fichier Parquet chargé sur S3 : {parquet_key}")
