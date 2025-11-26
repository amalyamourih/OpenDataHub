import requests
import os

def download_file(url: str, dest_path: str):
    """Télécharge un fichier depuis une URL"""
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024*1024):
                f.write(chunk)
    return dest_path
