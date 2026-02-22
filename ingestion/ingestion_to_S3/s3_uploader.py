import boto3
import os
import shutil
from utils.dictionnaire import DATA_FORMATS

# Construit un mapping inverse : extension → catégorie S3
# ex: "tsv" → "tabular", "geojson" → "geospatial_vector"
EXTENSION_TO_CATEGORY = {
    ext: category
    for category, formats in DATA_FORMATS.items()
    for ext in formats
}

def get_s3_category(filename: str) -> str:
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    return EXTENSION_TO_CATEGORY.get(ext, "others")  # fallback → others


def upload_folder_to_s3(root_folder: str, bucket_name: str, region: str):
    s3 = boto3.client("s3", region_name=region)
    uploaded = []
    errors = []

    for root, dirs, files in os.walk(root_folder):
        for file in files:
            local_path = os.path.join(root, file)
            category   = get_s3_category(file)
            s3_key     = f"{category}/{file}"   # ex: "tabular/mon_fichier.tsv"

            print(f"→ Upload : {local_path}  ──►  s3://{bucket_name}/{s3_key}")

            try:
                s3.upload_file(local_path, bucket_name, s3_key)
                uploaded.append(s3_key)

                # Suppression du fichier local après upload réussi
                os.remove(local_path)
                print(f"✓ Uploadé et supprimé localement : {file}")

            except Exception as e:
                errors.append((local_path, str(e)))
                print(f"✗ Erreur upload {file} : {e}")


    for item in os.listdir(root_folder):
        item_path = os.path.join(root_folder, item)
        if os.path.isdir(item_path):
            shutil.rmtree(item_path)

    # ── Rapport final ──────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print(f"{len(uploaded)} fichier(s) uploadé(s) avec succès")
    if errors:
        print(f"{len(errors)} erreur(s) :")
        for path, err in errors:
            print(f"  - {path} → {err}")
        raise RuntimeError(f"{len(errors)} fichier(s) n'ont pas pu être uploadés.")

    print(f"Le dossier '{root_folder}' est maintenant vide.")
    return uploaded