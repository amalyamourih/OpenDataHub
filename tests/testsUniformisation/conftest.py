"""
Configuration pytest pour les tests de conversion
Fixtures globales utilisées par tous les fichiers de test
"""

import pytest
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import numpy as np
import io
from unittest.mock import patch, MagicMock


# ============================================================================
# FIXTURE - DONNÉES DE TEST TABULAIRES
# ============================================================================

@pytest.fixture
def sample_df():
    """DataFrame de test standard"""
    return pd.DataFrame({
        'id': [1, 2, 3],
        'nom': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'ville': ['Paris', 'Lyon', 'Marseille']
    })


# ============================================================================
# FIXTURE - DONNÉES DE TEST GÉOSPATIALES
# ============================================================================

@pytest.fixture
def sample_geodf():
    """GeoDataFrame de test standard"""
    return gpd.GeoDataFrame({
        'id': [1, 2, 3],
        'name': ['A', 'B', 'C'],
        'value': [10, 20, 30],
        'geometry': [
            Point(2.3522, 48.8566),
            Point(4.8357, 45.7640),
            Point(-0.5792, 44.8378)
        ]
    }, crs='EPSG:4326')


# ============================================================================
# FIXTURE - DONNÉES DE TEST RASTER
# ============================================================================

@pytest.fixture
def sample_raster_data():
    """Données raster de test"""
    return {
        'data': np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]], dtype=np.float32),
        'width': 3,
        'height': 3,
        'crs': 'EPSG:4326'
    }


# ============================================================================
# FIXTURE - MOCK S3 CLIENT (VARIABLE GLOBALE)
# ============================================================================

@pytest.fixture
def mock_s3_client():
    """Mock du client S3 - patche la variable globale s3_client"""
    with patch('transformation.transforme_with_duckdb.transforme_to_parquet.s3_client') as mock_s3:
        mock_s3.get_object.return_value = {'Body': io.BytesIO(b'')}
        mock_s3.put_object.return_value = {}
        mock_s3.delete_object.return_value = {}
        yield mock_s3


# ============================================================================
# FIXTURE - MOCK BOTO3.CLIENT()
# ============================================================================

@pytest.fixture
def mock_boto3_client():
    """Mock boto3.client() pour les fonctions qui créent leur propre client"""
    with patch('transformation.transforme_with_duckdb.transforme_to_parquet.boto3.client') as mock_boto:
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3
        mock_s3.get_object.return_value = {'Body': io.BytesIO(b'')}
        mock_s3.put_object.return_value = {}
        mock_s3.delete_object.return_value = {}
        yield mock_s3


# ============================================================================
# FIXTURE - BUCKET S3 AVEC STOCKAGE EN MÉMOIRE
# ============================================================================

@pytest.fixture
def temp_s3_bucket_mock():
    """Mock d'un bucket S3 avec stockage en mémoire"""
    bucket_storage = {}
    
    def mock_put_object(**kwargs):
        key = kwargs.get('Key', '')
        body = kwargs.get('Body', b'')
        bucket_storage[key] = body
        return {}
    
    def mock_get_object(**kwargs):
        key = kwargs.get('Key', '')
        if key in bucket_storage:
            data = bucket_storage[key]
            if isinstance(data, io.BytesIO):
                data = data.getvalue()
            return {'Body': io.BytesIO(data)}
        return {'Body': io.BytesIO(b'')}
    
    def mock_delete_object(**kwargs):
        key = kwargs.get('Key', '')
        if key in bucket_storage:
            del bucket_storage[key]
        return {}
    
    mock_client = MagicMock()
    mock_client.put_object.side_effect = mock_put_object
    mock_client.get_object.side_effect = mock_get_object
    mock_client.delete_object.side_effect = mock_delete_object
    mock_client.storage = bucket_storage
    
    yield mock_client
    bucket_storage.clear()