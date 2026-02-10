import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ingestion.ingestion_to_S3.s3_uploader import upload_folder_to_s3
from utils.config import S3_BUCKET, AWS_REGION

def test_upload():
    upload_folder_to_s3("data_temp", S3_BUCKET, AWS_REGION)
    print(f"Test upload OK (vérifier sur le bucket S3 : {S3_BUCKET})")


if __name__ == "__main__":
    test_upload()
