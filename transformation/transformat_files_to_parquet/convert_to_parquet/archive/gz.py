from ingestion.s3.io import read_s3_object, write_s3_object, delete_s3_object
import gzip
import os
from utils.config import S3_BUCKET


def convert_gz_to_parquet(path_to_gz_key, S3_BUCKET=S3_BUCKET):
    from transformation.transformat_files_to_parquet.convert_to_parquet.converts.convert_by_extension import _convert_by_extension
    content = read_s3_object(path_to_gz_key)
    # Décompresser
    decompressed = gzip.decompress(content)
    
    # Déterminer l'extension du fichier décompressé
    filename = os.path.basename(path_to_gz_key)
    if filename.endswith('.gz'):
        inner_filename = filename[:-3]
    else:
        inner_filename = filename + ".decompressed"
    
    ext = inner_filename.rsplit('.', 1)[-1].lower() if '.' in inner_filename else 'txt'
    
    temp_key = f"temp_extracted/{inner_filename}"
    write_s3_object(temp_key, decompressed)
    _convert_by_extension(temp_key, ext, S3_BUCKET)
    delete_s3_object(temp_key)
    
    print(f"GZ décompressé et converti : {path_to_gz_key}")