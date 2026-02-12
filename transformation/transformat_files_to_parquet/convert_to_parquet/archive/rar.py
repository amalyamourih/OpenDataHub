from ingestion.s3.io import read_s3_object, write_s3_object, delete_s3_object
import os
import tempfile
import rarfile
from utils.config import S3_BUCKET


def convert_rar_to_parquet(path_to_rar_key, S3_BUCKET=S3_BUCKET):
    from transformation.transformat_files_to_parquet.convert_to_parquet.converts.convert_by_extension import _convert_by_extension
    content = read_s3_object(path_to_rar_key)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        archive_path = os.path.join(tmpdir, "archive.rar")
        with open(archive_path, 'wb') as f:
            f.write(content)
        
        with rarfile.RarFile(archive_path) as rf:
            rf.extractall(tmpdir)
        
        for root, dirs, files in os.walk(tmpdir):
            for file in files:
                if file == "archive.rar":
                    continue
                file_path = os.path.join(root, file)
                ext = file.rsplit('.', 1)[-1].lower() if '.' in file else ''
                
                with open(file_path, 'rb') as f:
                    file_content = f.read()
                
                temp_key = f"temp_extracted/{file}"
                write_s3_object(temp_key, file_content)
                _convert_by_extension(temp_key, ext, S3_BUCKET)
                delete_s3_object(temp_key)
    
    print(f"RAR extrait et converti : {path_to_rar_key}")