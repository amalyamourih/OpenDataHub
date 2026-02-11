import os
import tempfile
import rasterio
import pandas as pd
from transformation.transformat_files_to_parquet.parquet.writer import dataframe_to_parquet_bytes
from ingestion.s3.io import read_s3_object, write_s3_object
from utils.config import S3_BUCKET


def convert_tiff_to_parquet(path_to_tiff_key, S3_BUCKET=S3_BUCKET):
    content = read_s3_object(path_to_tiff_key)
    
    with tempfile.NamedTemporaryFile(suffix=".tiff", delete=False) as tmp:
        tmp.write(content)
        tmp_path = tmp.name
    
    try:
        with rasterio.open(tmp_path) as src:
            # Lire les métadonnées
            meta = {
                'crs': str(src.crs),
                'transform': str(src.transform),
                'width': src.width,
                'height': src.height,
                'count': src.count,
                'dtype': str(src.dtypes[0])
            }
            
            # Lire les données raster et les aplatir
            records = []
            for band_idx in range(1, src.count + 1):
                band_data = src.read(band_idx)
                for row in range(src.height):
                    for col in range(src.width):
                        x, y = src.xy(row, col)
                        records.append({
                            'band': band_idx,
                            'row': row,
                            'col': col,
                            'x': x,
                            'y': y,
                            'value': float(band_data[row, col])
                        })
            
            df = pd.DataFrame(records)
            # Ajouter les métadonnées comme colonnes constantes
            for key, val in meta.items():
                df[f'meta_{key}'] = str(val)
        
        
        parquet_buffer = dataframe_to_parquet_bytes(df)
        parquet_key = f"parquets_files/{os.path.basename(path_to_tiff_key).rsplit('.', 1)[0]}.parquet"
        write_s3_object(parquet_key, parquet_buffer.read())
        
        print(f"TIFF converti : {parquet_key}")
        
    finally:
        os.remove(tmp_path)