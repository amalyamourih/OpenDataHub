import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from ingestion.ingestion_to_S3.downloader import download_file
from ingestion.ingestion_to_S3.datagouv_client import get_dataset_metadata, find_resource_for_format
from utils.config import DATASET_SLUG


def test_download_files():
    # Récupérer les métadonnées du dataset
    dataset_meta = get_dataset_metadata(DATASET_SLUG)

    # Chercher les ressources 
    list_resource = find_resource_for_format(dataset_meta)
    assert list_resource is not None, "Aucun fichier trouvé dans le dataset"

    # Télécharger le fichier
    downloaded_path_data_temp = download_file(list_resource)

    # Vérifications
    assert os.path.exists(downloaded_path_data_temp), "Le fichier n'a pas été téléchargé"
    assert os.path.getsize(downloaded_path_data_temp) > 0, "Le fichier téléchargé est vide"

    print("Test downloader OK")

if __name__ == "__main__":
    test_download_files()
