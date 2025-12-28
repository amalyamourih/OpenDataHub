import boto3
from src.config import  S3_BUCKET, AWS_REGION 
# recuprer la liste des object dans un bucket s3
def list_s3_objects(bucket_name):
    s3 = boto3.client('s3')
    bucket_name = S3_BUCKET
    response = s3.list_objects_v2(Bucket=bucket_name)

    return list(obj['Key'] for obj in response.get('Contents',[]))


