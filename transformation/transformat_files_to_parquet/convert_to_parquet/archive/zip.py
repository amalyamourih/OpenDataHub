from ingestion.s3.io import read_s3_object, write_s3_object, delete_s3_object
import os
import tempfile
import zipfile
from transformation.transformat_files_to_parquet.convert_to_parquet.converts.convert_by_extension import _convert_by_extension
from utils.config import S3_BUCKET

def convert_zip_to_parquet(path_to_zip_key, S3_BUCKET=S3_BUCKET):
    content = read_s3_object(path_to_zip_key)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "archive.zip")
        with open(zip_path, 'wb') as f:
            f.write(content)
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(tmpdir)
        
        # Parcourir les fichiers extraits
        for root, dirs, files in os.walk(tmpdir):
            for file in files:
                if file == "archive.zip":
                    continue
                file_path = os.path.join(root, file)
                ext = file.rsplit('.', 1)[-1].lower() if '.' in file else ''
                
                # Upload le fichier extrait sur S3 puis convertir
                with open(file_path, 'rb') as f:
                    file_content = f.read()
                
                temp_key = f"temp_extracted/{file}"
                write_s3_object(temp_key, file_content)
                
                # Appeler la fonction de conversion appropriée
                _convert_by_extension(temp_key, ext, S3_BUCKET)
                
                # Nettoyer le fichier temporaire
                delete_s3_object(temp_key)
    
    print(f"ZIP extrait et converti : {path_to_zip_key}")