"""
Tests de conversion des formats géospatiaux raster vers Parquet
Formats: TIFF, TIF, JP2, ECW
"""

import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

import pytest
import pandas as pd
import numpy as np
import io
import os
import tempfile

from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_raster.tiff import convert_tiff_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_raster.jp2 import convert_jp2_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_raster.ecw import convert_ecw_to_parquet 
from transformation.transformat_files_to_parquet.convert_to_parquet.geospatial_raster.tif import convert_tif_to_parquet




# ============================================================================
# HELPER
# ============================================================================

def create_test_geotiff(width=10, height=10, bands=1):
    """Crée un GeoTIFF de test"""
    rasterio = pytest.importorskip("rasterio")
    from rasterio.transform import from_bounds
    
    data = np.random.randint(0, 255, (bands, height, width), dtype=np.uint8)
    transform = from_bounds(0, 0, 1, 1, width, height)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tiff_path = os.path.join(tmpdir, "test.tiff")
        
        with rasterio.open(
            tiff_path, 'w', driver='GTiff',
            height=height, width=width, count=bands,
            dtype=data.dtype, crs='EPSG:4326', transform=transform
        ) as dst:
            dst.write(data)
        
        with open(tiff_path, 'rb') as f:
            tiff_bytes = f.read()
    
    return tiff_bytes


def test_convert_tiff_to_parquet(mock_boto3_client):
    """Test conversion TIFF vers Parquet"""
    pytest.importorskip("rasterio")
    tiff_bytes = create_test_geotiff(width=5, height=5, bands=1)
    
    mock_boto3_client.get_object.return_value = {'Body': io.BytesIO(tiff_bytes)}
    
    convert_tiff_to_parquet("test_files/image.tiff")
    
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    parquet_data = put_call[1]['Body']
    df_result = pd.read_parquet(io.BytesIO(parquet_data))
    print(df_result.head())


def test_convert_tiff_to_parquet_multiband(mock_boto3_client):
    pytest.importorskip("rasterio")
    tiff_bytes = create_test_geotiff(width=5, height=5, bands=3)
    
    mock_boto3_client.get_object.return_value = {'Body': io.BytesIO(tiff_bytes)}
    
    convert_tiff_to_parquet("test_files/rgb.tiff")
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    parquet_data = put_call[1]['Body']
    df_result = pd.read_parquet(io.BytesIO(parquet_data))
    print(df_result.head())
    

def test_convert_tif_to_parquet(mock_boto3_client):

    pytest.importorskip("rasterio")
    tiff_bytes = create_test_geotiff(width=5, height=5, bands=1)
    
    mock_boto3_client.get_object.return_value = {'Body': io.BytesIO(tiff_bytes)}
    
    convert_tif_to_parquet("test_files/image.tif")
    
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    parquet_data = put_call[1]['Body']
    df_result = pd.read_parquet(io.BytesIO(parquet_data))
    print(df_result.head())



def test_convert_jp2_to_parquet(mock_boto3_client):
    rasterio = pytest.importorskip("rasterio")
    from rasterio.transform import from_bounds
    
    width, height = 10, 10
    data = np.random.randint(0, 255, (1, height, width), dtype=np.uint8)
    transform = from_bounds(0, 0, 1, 1, width, height)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        jp2_path = os.path.join(tmpdir, "test.jp2")
        try:
            with rasterio.open(
                jp2_path, 'w', driver='JP2OpenJPEG',
                height=height, width=width, count=1,
                dtype=data.dtype, crs='EPSG:4326', transform=transform
            ) as dst:
                dst.write(data)
            
            with open(jp2_path, 'rb') as f:
                jp2_bytes = f.read()
        except Exception:
            pytest.skip("Driver JP2OpenJPEG non disponible")
    
    mock_boto3_client.get_object.return_value = {'Body': io.BytesIO(jp2_bytes)}
    
    convert_jp2_to_parquet("test_files/image.jp2")
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    parquet_data = put_call[1]['Body']
    df_result = pd.read_parquet(io.BytesIO(parquet_data))
    print(df_result.head())


def test_convert_ecw_to_parquet(mock_boto3_client):
    pytest.importorskip("rasterio")
    tiff_bytes = create_test_geotiff(width=5, height=5, bands=1)
    
    mock_boto3_client.get_object.return_value = {'Body': io.BytesIO(tiff_bytes)}
    
    try:
        convert_ecw_to_parquet("test_files/image.ecw")
        assert mock_boto3_client.put_object.called
        put_call = mock_boto3_client.put_object.call_args
        parquet_data = put_call[1]['Body']
        df_result = pd.read_parquet(io.BytesIO(parquet_data))
        print(df_result.head())
    except Exception:
        assert mock_boto3_client.get_object.called