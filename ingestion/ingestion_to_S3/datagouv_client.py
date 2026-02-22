import requests
import sys
import os 
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from typing import Dict, List, Optional
from utils.config import DATA_GOUV_API_ROOT, DATASET_SLUG
from utils.dictionnaire import DATA_FORMATS

from datetime import datetime

def get_dataset_metadata(slug: str = DATASET_SLUG) -> Dict:
    """Récupère les métadonnées du dataset depuis data.gouv.fr"""
    url = f"{DATA_GOUV_API_ROOT}/datasets/{slug}/"
    response = requests.get(url,timeout=60)
    response.raise_for_status()
    return response.json()


def get_date_time_current():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def find_resource_for_format(dataset_meta: Dict, categories=("tabular","geospatial_vector","geospatial_raster","databases","archives","others")) -> List[Dict]:
    """Cherche les fichier dans les ressources du dataset"""
    prefer_formats = []
    ressource_results = []
    for cat in categories:
        prefer_formats.extend(DATA_FORMATS.get(cat, {}).keys())

    for r in dataset_meta.get("resources", []):
        r_format = r.get("format", "").lower()
        title = r.get("title", "")
        url = r.get("url", "")
        url_ext = os.path.splitext(url)[1].lower().lstrip(".")
        file_name = os.path.basename(url)

        for fmt in prefer_formats:
            if r_format == fmt or url_ext == fmt:
                cat_found = next((c for c in categories if fmt in DATA_FORMATS.get(c, {})), "unknown")
                
                path = f"{cat_found}/{fmt}/"
                ressource_results.append([r_format, url, path, title, get_date_time_current()])

    return ressource_results

def list_last_updated_dataset_slugs(limit: int = 10) -> list[str]:
    """Retourne les slugs des `limit` derniers datasets mis à jour sur data.gouv.fr."""
    url = f"{DATA_GOUV_API_ROOT}/datasets/"
    params = {"sort": "-last_update", "page_size": limit}
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()
    return [d["slug"] for d in data.get("data", []) if "slug" in d]