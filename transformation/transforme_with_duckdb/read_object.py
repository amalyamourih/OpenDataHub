from utils.config import S3_BUCKET

import boto3
# Je pense que c'est pas la peine de recuperer tout les meta donnée de file , A verifier !!!
def read_meta_data_(path_to_key):
    s3 = boto3.client('s3')
    file = s3.get_object(Bucket=S3_BUCKET, Key=path_to_key)
    file_name = path_to_key.split('/')[-1]
    extension = path_to_key.split('.')[-1].lower()
    
    return file , file_name , extension, path_to_key
