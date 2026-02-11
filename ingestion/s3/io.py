import io
from utils.config import S3_BUCKET
from client import get_s3_client

def read_s3_object(key: str) -> bytes:
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return obj["Body"].read()

def write_s3_object(key: str, data: bytes):
    s3 = get_s3_client()
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=data)
