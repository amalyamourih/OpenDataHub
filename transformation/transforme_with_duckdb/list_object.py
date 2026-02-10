import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
src_path = os.path.join(project_root, "src")
sys.path.insert(0, src_path)
import boto3
from utils.config import  S3_BUCKET, AWS_REGION 
# recuprer la liste des object dans un bucket s3
def list_s3_objects(bucket_name):
    s3 = boto3.client('s3')
    bucket_name = S3_BUCKET
    response = s3.list_objects_v2(Bucket=bucket_name)
    return list(obj['Key'] for obj in response.get('Contents',[]))

