from ingestion.s3.io import read_s3_object, write_s3_object, delete_s3_object
import py7zr
import tempfile
import os
import boto3
from utils.config import S3_BUCKET


def convert_7z_to_parquet(path_to_7z_key, S3_BUCKET=S3_BUCKET):
    from transformation.transformat_files_to_parquet.convert_to_parquet.converts.convert_by_extension import _convert_by_extension
    content = read_s3_object(path_to_7z_key)
    s3_client = boto3.client('s3')
    
    with tempfile.TemporaryDirectory() as tmpdir:
        archive_path = os.path.join(tmpdir, "archive.7z")
        with open(archive_path, 'wb') as f:
            f.write(content)
        
        with py7zr.SevenZipFile(archive_path, mode='r') as z:
            z.extractall(path=tmpdir)
        
        for root, dirs, files in os.walk(tmpdir):
            for file in files:
                if file == "archive.7z":
                    continue
                file_path = os.path.join(root, file)
                ext = file.rsplit('.', 1)[-1].lower() if '.' in file else ''
                
                with open(file_path, 'rb') as f:
                    file_content = f.read()
                
                temp_key = f"temp_extracted/{file}"
                write_s3_object(temp_key, file_content)
                _convert_by_extension(temp_key, ext, S3_BUCKET)
                delete_s3_object(temp_key)
    
    print(f"7Z extrait et converti : {path_to_7z_key}")