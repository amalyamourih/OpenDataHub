# tests/test_downloader.py
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ingestion.ingestion_to_S3.downloader import download_file
from ingestion.ingestion_to_S3.datagouv_client import get_dataset_metadata, find_resource_for_format
from utils.config import DATASET_SLUG


def test_download_files():
    dataset_meta = get_dataset_metadata(DATASET_SLUG)
    resources = find_resource_for_format(dataset_meta)
    assert resources, "Aucun fichier trouvé dans le dataset"

    out_dir = download_file(resources)  # assume it returns a directory path
    assert os.path.isdir(out_dir), f"download_file doit retourner un dossier. Reçu: {out_dir}"

    files = [p for p in Path(out_dir).rglob("*") if p.is_file()]
    assert files, "Aucun fichier créé dans le dossier de téléchargement"

    non_empty = [p for p in files if p.stat().st_size > 0]
    assert non_empty, "Tous les fichiers téléchargés sont vides"

    print(f"OK: {len(non_empty)}/{len(files)} fichiers non vides dans {out_dir}")
    