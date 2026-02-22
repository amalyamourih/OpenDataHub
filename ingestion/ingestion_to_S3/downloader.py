import requests
import os
from requests.exceptions import HTTPError, RequestException
from urllib.parse import urlparse  

def download_file(list_resource, dest_path_data_temp="data_temp/"):
    errors = []
    skipped = []

    for resource in list_resource:
        url        = resource[1]
        dest_path  = resource[2]

        parsed    = urlparse(url)
        file_name = os.path.basename(parsed.path.rstrip("/"))
        if not file_name:
            file_name = "downloaded_file"

        full_dir  = os.path.join(dest_path_data_temp, dest_path)
        full_path = os.path.join(full_dir, file_name)
        os.makedirs(full_dir, exist_ok=True)

        try:
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(full_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
            print(f"Téléchargé : {file_name}")

        except HTTPError as e:
            status = getattr(e.response, "status_code", None)
            if status == 404:
                print(f"404 ignoré : {url}")
                skipped.append(url)
                continue          
            else:
                errors.append((url, f"HTTP {status}: {e}"))
                continue         

        except RequestException as e:
            errors.append((url, f"Request error: {e}"))
            continue

    # ── Rapport final ─────────────────────────────────────────────────────────
    if skipped:
        print(f"\n{len(skipped)} fichier(s) ignoré(s) (404):")
        for url in skipped:
            print(f"  - {url}")

    if errors:
        print(f"\n{len(errors)} erreur(s) inattendue(s):")
        for url, err in errors:
            print(f"  - {url} → {err}")

    if errors and not os.listdir(dest_path_data_temp):
        raise RuntimeError(
            f"Aucun fichier téléchargé avec succès. Erreurs: {errors}"
        )

    return dest_path_data_temp