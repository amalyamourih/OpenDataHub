import os
import io
import boto3
import geopandas as gpd
from utils.config import S3_BUCKET
from ingestion.s3.io import read_s3_object, write_s3_object
from transformation.transformat_files_to_parquet.parquet.writer import dataframe_to_parquet_bytes


def convert_kmz_to_parquet(path_to_kmz_key, S3_BUCKET=S3_BUCKET):
    content = read_s3_object(path_to_kmz_key)
    gdf = gpd.read_file(content, driver="KML")

    parquet_buffer = dataframe_to_parquet_bytes(gdf)

    parquet_key = f"parquets_files/{os.path.basename(path_to_kmz_key).rsplit('.',1)[0]}.parquet"
    write_s3_object(parquet_key, parquet_buffer)

    print(f"KMZ converti : {parquet_key}")
