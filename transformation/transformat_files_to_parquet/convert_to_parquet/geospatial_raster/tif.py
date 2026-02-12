
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_raster.tiff import convert_tiff_to_parquet
from utils.config import S3_BUCKET

def convert_tif_to_parquet(path_to_tif_key, S3_BUCKET=S3_BUCKET):

    convert_tiff_to_parquet(path_to_tif_key, S3_BUCKET=S3_BUCKET)