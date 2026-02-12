import io
import geopandas as gpd
from ingestion.s3.io import read_s3_object, write_s3_object
from transformation.transformat_files_to_parquet.parquet.writer import dataframe_to_parquet_bytes
from utils.config import S3_BUCKET
import os

def convert_geojson_to_parquet(path_to_geojson_key, S3_BUCKET=S3_BUCKET):
    content = read_s3_object(path_to_geojson_key)
    gdf = gpd.read_file(content)

    parquet_buffer = dataframe_to_parquet_bytes(gdf)

    parquet_key = f"parquets_files/{os.path.basename(path_to_geojson_key).rsplit('.',1)[0]}.parquet"
    write_s3_object(parquet_key, parquet_buffer)

    print(f"GeoJSON converti : {parquet_key}")
