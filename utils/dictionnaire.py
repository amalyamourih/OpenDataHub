DATA_FORMATS = {
    "tabular": {
        "csv": {"type": "tableau", "description": "Comma-Separated Values, texte séparé par des virgules"},
        "tsv": {"type": "tableau", "description": "Tab-Separated Values, texte séparé par des tabulations"},
        "xls": {"type": "tableau", "description": "Fichier Excel ancien format"},
        "xlsx": {"type": "tableau", "description": "Fichier Excel moderne"},
        "ods": {"type": "tableau", "description": "OpenDocument Spreadsheet, tableur open-source"},
        "txt": {"type": "texte", "description": "Texte brut"},
        "dbf": {"type": "tableau", "description": "Ancien format de base/tabulaire dBase"},
        "parquet": {"type": "tableau", "description": "Fichier colonne optimisé pour le Big Data"},
        "pq": {"type": "tableau", "description": "Alias pour Parquet"}
    },
    
    "geospatial_vector": {
        "shp": {"type": "vecteur", "description": "Shapefile, format géographique vecteur"},
        "shz": {"type": "vecteur", "description": "Shapefile compressé"},
        "geojson": {"type": "vecteur", "description": "Format JSON pour données géographiques"},
        "kml": {"type": "vecteur", "description": "Keyhole Markup Language, Google Earth"},
        "kmz": {"type": "vecteur", "description": "Version compressée du KML"},
        "gpkg": {"type": "vecteur", "description": "GeoPackage, format vecteur moderne"},
        "dwg": {"type": "vecteur", "description": "Format DAO/CAO"},
        "dxf": {"type": "vecteur", "description": "Format DAO/CAO"}
    },
    
    "geospatial_raster": {
        "tiff": {"type": "raster", "description": "GeoTIFF, image géospatiale"},
        "tif": {"type": "raster", "description": "Alias de TIFF"},
        "jp2": {"type": "raster", "description": "JPEG2000 pour images géospatiales"},
        "ecw": {"type": "raster", "description": "Format compressé pour images géospatiales"}
    },
    
    "databases": {
        "sql": {"type": "base", "description": "Script SQL / dump de base de données"},
        "db": {"type": "base", "description": "Base SQLite ou autre base légère"}
    },
    
    "archives": {
        "zip": {"type": "archive", "description": "Archive compressée"},
        "tar": {"type": "archive", "description": "Archive Unix"},
        "tgz": {"type": "archive", "description": "Archive tar compressée gzip"},
        "gz": {"type": "archive", "description": "Gzip"},
        "xz": {"type": "archive", "description": "Compression XZ"},
        "7z": {"type": "archive", "description": "7-Zip"},
        "bz2": {"type": "archive", "description": "Bzip2"},
        "rar": {"type": "archive", "description": "RAR"}
    },
    
    "others": {
        "json": {"type": "structuré", "description": "Données structurées en JSON"},
        "xml": {"type": "structuré", "description": "Données hiérarchiques en XML"},
        "pdf": {"type": "document", "description": "Document PDF, souvent non tabulaire"},
        "docx": {"type": "document", "description": "Document Word"},
        "odt": {"type": "document", "description": "OpenDocument Text, document open-source"}
    }
}
