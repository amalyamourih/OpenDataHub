import os
import geopandas as gpd
import tempfile
from utils.config import S3_BUCKET
from ingestion.s3.io import read_s3_object, write_s3_object
from transformation.transformat_files_to_parquet.parquet.writer import dataframe_to_parquet_bytes


def convert_shz_to_parquet(path_to_shz_key, S3_BUCKET=S3_BUCKET):
    content = read_s3_object(path_to_shz_key)
    with tempfile.TemporaryDirectory() as tmpdir:
        shz_path = os.path.join(tmpdir, "file.shz")
        with open(shz_path, "wb") as f:
            f.write(content)
        gdf = gpd.read_file(shz_path)

        parquet_buffer = dataframe_to_parquet_bytes(gdf)
        parquet_key = f"parquets_files/{os.path.basename(path_to_shz_key).rsplit('.',1)[0]}.parquet"
        write_s3_object(parquet_key, parquet_buffer)

        print(f"SHZ converti : {parquet_key}")