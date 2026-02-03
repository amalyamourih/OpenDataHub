import boto3
import os 

# Acceder au service S3
s3_client = boto3.client('s3')

# Recuperer la liste des cles des object à partir de la bucket S3
def list_s3_keys(bucket, prefix="parquets_files/"):
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # Pour vérfieri que c'est un fichier 
            if not key.endswith("/"):
                keys.append(key)

    return keys

# Recuperer la nom de la table a partir du clé de l'objet 
def get_table_name_from_key(key):
    filename = os.path.basename(key)
    table_name = os.path.splitext(filename)[0]
    return table_name
