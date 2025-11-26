import boto3
import os

def upload_file_to_s3(local_file, bucket_name, s3_key, region):
    s3 = boto3.client('s3', region_name=region)
    try:
        s3.upload_file(local_file, bucket_name, s3_key)
        print(f"Fichier uploadé : s3://{bucket_name}/{s3_key}")
        
        if os.path.exists(local_file):
            os.remove(local_file)
            print(f"Fichier local supprimé : {local_file}")
        else:
            print(f"Le fichier {local_file} n'existe pas localement.")
    except Exception as e:
        print("Erreur lors de l'upload :", e)
