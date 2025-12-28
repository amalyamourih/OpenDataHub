from src.transforme_with_duckdb.list_object import list_s3_objects
from src.config import S3_BUCKET

def test_extract_object(bucket_name):
    list_of_files = list_s3_objects(bucket_name)
    for list in list_of_files: 
        print(list)


if __name__ == "__main__":
    bucket_name = S3_BUCKET
    test_extract_object(bucket_name)