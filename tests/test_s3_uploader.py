from src.s3_uploader import upload_file_to_s3
from src.config import S3_BUCKET, AWS_REGION

def test_upload():
    local_file = "data_temp/POPULATION_MUNICIPALE_COMMUNES_FRANCE.xlsx.xlsx"
    s3_key = "xlsx/POPULATION_MUNICIPALE_COMMUNES_FRANCE.xlsx.xlsx"
    upload_file_to_s3(local_file, S3_BUCKET, s3_key, AWS_REGION)
    print(f"Test upload OK (vérifier sur le bucket S3 : {S3_BUCKET})")


if __name__ == "__main__":
    test_upload()
