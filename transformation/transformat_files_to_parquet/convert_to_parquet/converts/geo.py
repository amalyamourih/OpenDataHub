import io
import os
import tempfile
import geopandas as gpd
from abc import ABC
from .base import BaseConverter
from ingestion.s3.io import write_s3_object
from transformation.transformat_files_to_parquet.parquet.writer import dataframe_to_parquet_bytes


class GeoToParquet(BaseConverter, ABC):

    def _export(self, gdf, s3_key: str):
        parquet_bytes = dataframe_to_parquet_bytes(gdf)
        parquet_key = (
            f"parquets_files/"
            f"{os.path.basename(s3_key).rsplit('.', 1)[0]}.parquet"
        )

        write_s3_object(parquet_key, parquet_bytes)
        print(f"Geo converti : {parquet_key}")
