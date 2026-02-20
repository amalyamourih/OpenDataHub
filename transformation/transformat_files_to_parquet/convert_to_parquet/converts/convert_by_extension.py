import boto3
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.csv import convert_csv_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.dbf import convert_dbf_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.ods import convert_ods_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.tsv import convert_tsv_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.txt import convert_txt_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.xls import convert_xls_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.xlsx import convert_xlsx_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.parquet import convert_parquet_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.pq import convert_pq_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_vector.dxf import convert_dxf_to_parquet 
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_vector.gpkg import convert_gpkg_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_vector.geojson import convert_geojson_to_parquet 
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_vector.kml import convert_kml_to_parquet 
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_vector.kmz import convert_kmz_to_parquet 
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_vector.shp import convert_shp_to_parquet 
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_vector.shz import convert_shz_to_parquet 
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_raster.ecw import convert_ecw_to_parquet 
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_raster.jp2 import convert_jp2_to_parquet 
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_raster.tif import convert_tif_to_parquet 
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_raster.tiff import convert_tiff_to_parquet 

from transformation.transformat_files_to_parquet.convert_to_parquet.databases.db import convert_db_to_parquet_all_tables
from transformation.transformat_files_to_parquet.convert_to_parquet.databases.sql import convert_sql_to_parquet 
from transformation.transformat_files_to_parquet.convert_to_parquet.documents.docx import convert_docx_to_parquet 
from transformation.transformat_files_to_parquet.convert_to_parquet.documents.odt import convert_odt_to_parquet 
from transformation.transformat_files_to_parquet.convert_to_parquet.documents.pdf import convert_pdf_to_parquet 
from transformation.transformat_files_to_parquet.convert_to_parquet.others.json import convert_json_to_parquet 
from transformation.transformat_files_to_parquet.convert_to_parquet.others.xml import convert_xml_to_parquet 
from transformation.transformat_files_to_parquet.convert_to_parquet.archive.bz2 import convert_bz2_to_parquet 
from transformation.transformat_files_to_parquet.convert_to_parquet.archive.gz import convert_gz_to_parquet 
from transformation.transformat_files_to_parquet.convert_to_parquet.archive.rar import convert_rar_to_parquet 
from transformation.transformat_files_to_parquet.convert_to_parquet.archive.tar import convert_tar_to_parquet 
from transformation.transformat_files_to_parquet.convert_to_parquet.archive.tgz import convert_tgz_to_parquet 
from transformation.transformat_files_to_parquet.convert_to_parquet.archive.xz import convert_xz_to_parquet 
from transformation.transformat_files_to_parquet.convert_to_parquet.archive.zip import convert_zip_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.archive._7z import convert_7z_to_parquet


from utils.config import S3_BUCKET

def _convert_by_extension(file_key, ext, S3_BUCKET=S3_BUCKET):
    """Route vers la fonction de conversion appropriée selon l'extension"""
    
    converters = {
        # Tabular
        'csv': convert_csv_to_parquet,
        'tsv': convert_tsv_to_parquet,
        'xlsx': convert_xlsx_to_parquet,
        'xls': convert_xls_to_parquet,
        'ods': convert_ods_to_parquet,
        'txt': convert_txt_to_parquet,
        'dbf': convert_dbf_to_parquet,
        'parquet': convert_parquet_to_parquet,
        'pq': convert_pq_to_parquet,
        
        # Geospatial Vector
        'shp': convert_shp_to_parquet,
        'shz': convert_shz_to_parquet,
        'geojson': convert_geojson_to_parquet,
        'kml': convert_kml_to_parquet,
        'kmz': convert_kmz_to_parquet,
        'gpkg': convert_gpkg_to_parquet,
        'dxf': convert_dxf_to_parquet,
        
        # Geospatial Raster
        'tiff': convert_tiff_to_parquet,
        'tif': convert_tif_to_parquet,
        'jp2': convert_jp2_to_parquet,
        'ecw': convert_ecw_to_parquet,
        
        # Databases
        'db': convert_db_to_parquet_all_tables,
        'sql': convert_sql_to_parquet,
        
        # Documents
        'pdf': convert_pdf_to_parquet,
        'docx': convert_docx_to_parquet,
        'odt': convert_odt_to_parquet,
        
        # Data formats
        'json': lambda key, bucket=S3_BUCKET: convert_json_to_parquet(key, lines=False, S3_BUCKET=bucket),
        'jsonl': lambda key, bucket=S3_BUCKET: convert_json_to_parquet(key, lines=True, S3_BUCKET=bucket),
        'xml': convert_xml_to_parquet,
        
        # Archives (récursif)
        'zip': convert_zip_to_parquet,
        'tar': convert_tar_to_parquet,
        'tgz': convert_tgz_to_parquet,
        'gz': convert_gz_to_parquet,
        'xz': convert_xz_to_parquet,
        'bz2': convert_bz2_to_parquet,
        '7z': convert_7z_to_parquet,
        'rar': convert_rar_to_parquet,
    }
    
    ext_lower = ext.lower()
    
    if ext_lower in converters:
        try:
            converters[ext_lower](file_key, S3_BUCKET)
            print(f" Converti: {file_key}")
        except Exception as e:
            print(f" Erreur pour {file_key}: {e}")
    else:
        print(f" Extension non supportée: .{ext} ({file_key})")