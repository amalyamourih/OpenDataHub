import tempfile
import os
import io
import geopandas as gpd
import boto3
from utils.config import S3_BUCKET


def convert_shp_to_parquet(path_to_shp_key, S3_BUCKET=S3_BUCKET):
    s3_client = boto3.client('s3')
    extensions = ['.shp', '.shx', '.dbf', '.prj', '.cpg']
    base_key = path_to_shp_key.rsplit('.', 1)[0]
    
    with tempfile.TemporaryDirectory() as tmpdir:
        base_local = os.path.join(tmpdir, "file")
        
        for ext in extensions:
            s3_key = base_key + ext
            local_path = base_local + ext
            try:
                obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
                with open(local_path, 'wb') as f:
                    f.write(obj['Body'].read())
            except s3_client.exceptions.NoSuchKey:
                if ext in ['.shp', '.shx', '.dbf']:
                    raise
        
        # Lire le shapefile complet
        gdf = gpd.read_file(base_local + ".shp")
        
        parquet_buffer = io.BytesIO()
        gdf.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        
        parquet_key = f"parquets_files/{os.path.basename(path_to_shp_key).rsplit('.', 1)[0]}.parquet"
        s3_client.put_object(Bucket=S3_BUCKET, Key=parquet_key, Body=parquet_buffer.getvalue())
        print(f"SHP converti : {parquet_key}")