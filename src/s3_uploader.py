import boto3
import os
import shutil

def upload_folder_to_s3(root_folder, bucket_name, region):
    s3 = boto3.client('s3', region_name=region)
    for root , dirs , files in os.walk(root_folder):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, root_folder)
            try:
                s3.upload_file(local_path, bucket_name, relative_path.replace("\\", "/"))
                if os.path.exists(local_path):
                    os.remove(local_path)
                    print(f"Fichier local supprimé : {local_path}")
                else:
                    print(f"Le fichier {local_path} n'existe pas localement.")
            except Exception as e:
                print("Erreur lors de l'upload :", e)

    for item in os.listdir(root_folder):
        item_path = os.path.join(root_folder, item)
        if os.path.isdir(item_path):
            shutil.rmtree(item_path)
    print(f"Le dossier '{root_folder}' est maintenant vide.")
