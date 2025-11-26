import os
from src.downloader import download_file
from src.datagouv_client import get_dataset_metadata, find_resource_for_format
from src.config import DATASET_SLUG

def test_download_excel():
    # 1️⃣ Récupérer les métadonnées du dataset
    dataset_meta = get_dataset_metadata(DATASET_SLUG)

    # 2️⃣ Chercher le fichier Excel
    resource_excel = find_resource_for_format(dataset_meta, prefer_formats=("xlsx", "xls"))
    assert resource_excel is not None, "Aucun fichier Excel trouvé dans le dataset"

    url = resource_excel.get("url")
    dest_path = f"data_temp/{resource_excel.get('title')}.xlsx"

    # 3️⃣ Télécharger le fichier
    downloaded_path = download_file(url, dest_path)

    # 4️⃣ Vérifications
    assert os.path.exists(downloaded_path), "Le fichier n'a pas été téléchargé"
    assert os.path.getsize(downloaded_path) > 0, "Le fichier téléchargé est vide"

    print("Test downloader Excel OK")
    print(f"Fichier téléchargé : {downloaded_path}")

if __name__ == "__main__":
    test_download_excel()
