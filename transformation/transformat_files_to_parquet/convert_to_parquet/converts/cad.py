import io
import os
import ezdxf
import pandas as pd
from abc import ABC
from .base import BaseConverter
from src.ingestion_to_S3.s3.io import read_s3_object, write_s3_object
from src.ingestion_to_S3.parquet.writer import dataframe_to_parquet_bytes


class CADToParquet(BaseConverter, ABC):

    def _export(self, df, s3_key: str):
        parquet_bytes = dataframe_to_parquet_bytes(df)
        parquet_key = (
            f"parquets_files/"
            f"{os.path.basename(s3_key).rsplit('.', 1)[0]}.parquet"
        )

        write_s3_object(parquet_key, parquet_bytes)
        print(f"CAD converti : {parquet_key}")
