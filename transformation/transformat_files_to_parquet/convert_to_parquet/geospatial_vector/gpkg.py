import tempfile
import geopandas as gpd
import os

from ingestion.s3.io import read_s3_object, write_s3_object
from transformation.transformat_files_to_parquet.parquet.writer import dataframe_to_parquet_bytes
from utils.config import S3_BUCKET

def convert_gpkg_to_parquet(path_to_gpkg_key, S3_BUCKET=S3_BUCKET):
    content = read_s3_object(path_to_gpkg_key)
    with tempfile.TemporaryDirectory() as tmpdir:
        gpkg_path = os.path.join(tmpdir, "file.gpkg")
        with open(gpkg_path, 'wb') as f:
            f.write(content)
        
        gdf = gpd.read_file(gpkg_path)

    parquet_buffer = dataframe_to_parquet_bytes(gdf)

    parquet_key = f"parquets_files/{os.path.basename(path_to_gpkg_key).rsplit('.', 1)[0]}.parquet"
    write_s3_object(parquet_key, parquet_buffer.read())
    print(f"GPKG converti : {parquet_key}")
