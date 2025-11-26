import requests
from typing import Dict, Optional
from .config import DATA_GOUV_API_ROOT, DATASET_SLUG

def get_dataset_metadata(slug: str = DATASET_SLUG) -> Dict:
    """Récupère les métadonnées du dataset depuis data.gouv.fr"""
    url = f"{DATA_GOUV_API_ROOT}/datasets/{slug}/"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def find_resource_for_format(dataset_meta: Dict, prefer_formats=("xlsx", "xls")) -> Optional[Dict]:
    """Cherche le fichier CSV dans les ressources du dataset"""
    for fmt in prefer_formats:
        for r in dataset_meta.get("resources", []):
            if (r.get("format") or "").lower() == fmt:
                return r
    return None
