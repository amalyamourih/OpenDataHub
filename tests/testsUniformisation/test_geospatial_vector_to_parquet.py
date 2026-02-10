"""
Tests de conversion des formats géospatiaux vecteur vers Parquet
Formats: SHP, SHZ, GeoJSON, KML, KMZ, GPKG, DWG, DXF
"""

import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

import pytest
import geopandas as gpd
import io
import os
import tempfile
import zipfile
from shapely.geometry import Point, LineString, Polygon
import ezdxf
import pandas as pd
from unittest.mock import patch, MagicMock
from transformation.transforme_with_duckdb.conversion_to_parquet import (
    convert_shp_to_parquet,
    convert_shz_to_parquet,
    convert_geojson_to_parquet,
    convert_kml_to_parquet,
    convert_kmz_to_parquet,
    convert_gpkg_to_parquet,
    convert_dwg_to_parquet,
    convert_dxf_to_parquet
)

def test_convert_shp_to_parquet(sample_geodf, mock_boto3_client):
    with tempfile.TemporaryDirectory() as tmpdir:
        shp_path = os.path.join(tmpdir, "data.shp")
        sample_geodf.to_file(shp_path)
        
        shp_files = {}
        base_name = os.path.splitext(shp_path)[0]
        for ext in ['.shp', '.shx', '.dbf', '.prj', '.cpg']:
            file_path = base_name + ext
            if os.path.exists(file_path):
                with open(file_path, 'rb') as f:
                    shp_files[ext] = f.read()
    
    def mock_get_object(**kwargs):
        key = kwargs.get('Key', '')
        for ext, data in shp_files.items():
            if key.endswith(ext):
                return {'Body': io.BytesIO(data)}
        raise Exception("NoSuchKey")
    
    mock_boto3_client.get_object.side_effect = mock_get_object
    convert_shp_to_parquet("test_files/data.shp")
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    parquet_data = put_call[1]['Body']
    gdf_result = gpd.read_parquet(io.BytesIO(parquet_data))
    print(gdf_result.head())
    assert len(gdf_result) == 3
    assert 'geometry' in gdf_result.columns
  

def test_convert_shp_to_parquet_polygons(mock_boto3_client):
    gdf = gpd.GeoDataFrame({
        'id': [1, 2],
        'name': ['Zone A', 'Zone B'],
        'geometry': [
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            Polygon([(2, 2), (3, 2), (3, 3), (2, 3)])
        ]
    }, crs='EPSG:4326')
    
    with tempfile.TemporaryDirectory() as tmpdir:
        shp_path = os.path.join(tmpdir, "polygons.shp")
        gdf.to_file(shp_path)
        
        shp_files = {}
        base_name = os.path.splitext(shp_path)[0]
        for ext in ['.shp', '.shx', '.dbf', '.prj', '.cpg']:
            file_path = base_name + ext
            if os.path.exists(file_path):
                with open(file_path, 'rb') as f:
                    shp_files[ext] = f.read()
    
    def mock_get_object(**kwargs):
        key = kwargs.get('Key', '')
        for ext, data in shp_files.items():
            if key.endswith(ext):
                return {'Body': io.BytesIO(data)}
        raise Exception("NoSuchKey")
    
    mock_boto3_client.get_object.side_effect = mock_get_object
    convert_shp_to_parquet("test_files/polygons.shp")
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    parquet_data = put_call[1]['Body']
    gdf_result = gpd.read_parquet(io.BytesIO(parquet_data))
    print(gdf_result.head())
    assert len(gdf_result) == 2
    assert 'geometry' in gdf_result.columns


def test_convert_shz_to_parquet(sample_geodf, mock_boto3_client):
    with tempfile.TemporaryDirectory() as tmpdir:
        shp_path = os.path.join(tmpdir, "data.shp")
        sample_geodf.to_file(shp_path)
        
        shz_path = os.path.join(tmpdir, "data.shz")
        with zipfile.ZipFile(shz_path, 'w') as zf:
            base_name = os.path.splitext(shp_path)[0]
            for ext in ['.shp', '.shx', '.dbf', '.prj', '.cpg']:
                file_path = base_name + ext
                if os.path.exists(file_path):
                    zf.write(file_path, os.path.basename(file_path))
        
        with open(shz_path, 'rb') as f:
            shz_bytes = f.read()
    
    mock_boto3_client.get_object.return_value = {'Body': io.BytesIO(shz_bytes)}
    convert_shz_to_parquet("test_files/data.shz")
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    parquet_data = put_call[1]['Body']
    gdf_result = gpd.read_parquet(io.BytesIO(parquet_data))
    print(gdf_result.head())
    assert len(gdf_result) == 3
    assert 'geometry' in gdf_result.columns


def test_convert_geojson_to_parquet(sample_geodf, mock_boto3_client):
    geojson_str = sample_geodf.to_json()
    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(geojson_str.encode('utf-8'))
    }
    convert_geojson_to_parquet("test_files/data.geojson")
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    parquet_data = put_call[1]['Body']
    gdf_result = gpd.read_parquet(io.BytesIO(parquet_data))
    print(gdf_result.head())
    assert len(gdf_result) == 3
    assert 'geometry' in gdf_result.columns


def test_convert_geojson_to_parquet_linestrings(mock_boto3_client):
    gdf = gpd.GeoDataFrame({
        'id': [1, 2],
        'name': ['Route A', 'Route B'],
        'geometry': [
            LineString([(0, 0), (1, 1), (2, 0)]),
            LineString([(3, 3), (4, 4), (5, 3)])
        ]
    }, crs='EPSG:4326')
    
    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(gdf.to_json().encode('utf-8'))
    }
    
    convert_geojson_to_parquet("test_files/lines.geojson")
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    parquet_data = put_call[1]['Body']
    gdf_result = gpd.read_parquet(io.BytesIO(parquet_data))
    print(gdf_result.head())
    assert len(gdf_result) == 2
    assert 'geometry' in gdf_result.columns


def test_convert_kml_to_parquet(mock_boto3_client):
    kml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <kml xmlns="http://www.opengis.net/kml/2.2">
        <Document>
            <Placemark>
                <name>Point A</name>
                <Point><coordinates>2.3522,48.8566,0</coordinates></Point>
            </Placemark>
            <Placemark>
                <name>Point B</name>
                <Point><coordinates>4.8357,45.764,0</coordinates></Point>
            </Placemark>
        </Document>
    </kml>"""
    
    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(kml_content.encode('utf-8'))
    }
    
    convert_kml_to_parquet("test_files/data.kml")
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    parquet_data = put_call[1]['Body']
    gdf_result = gpd.read_parquet(io.BytesIO(parquet_data))
    print(gdf_result.head())
    assert len(gdf_result) == 2
    assert 'geometry' in gdf_result.columns


def test_convert_kml_to_parquet_polygon(mock_boto3_client):
    kml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <kml xmlns="http://www.opengis.net/kml/2.2">
        <Document>
            <Placemark>
                <name>Zone</name>
                <Polygon>
                    <outerBoundaryIs>
                        <LinearRing>
                            <coordinates>0,0,0 1,0,0 1,1,0 0,1,0 0,0,0</coordinates>
                        </LinearRing>
                    </outerBoundaryIs>
                </Polygon>
            </Placemark>
        </Document>
    </kml>"""
    
    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(kml_content.encode('utf-8'))
    }
    
    convert_kml_to_parquet("test_files/polygon.kml")
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    parquet_data = put_call[1]['Body']
    gdf_result = gpd.read_parquet(io.BytesIO(parquet_data))
    print(gdf_result.head())
    assert len(gdf_result) == 1
    assert 'geometry' in gdf_result.columns


def test_convert_kmz_to_parquet(mock_boto3_client):
    kml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <kml xmlns="http://www.opengis.net/kml/2.2">
        <Document>
            <Placemark>
                <name>Point A</name>
                <Point><coordinates>2.3522,48.8566,0</coordinates></Point>
            </Placemark>
        </Document>
    </kml>"""
    
    kmz_buffer = io.BytesIO()
    with zipfile.ZipFile(kmz_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('doc.kml', kml_content)
    kmz_buffer.seek(0)
    
    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(kmz_buffer.getvalue())
    }
    
    convert_kmz_to_parquet("test_files/data.kmz")
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    parquet_data = put_call[1]['Body']
    gdf_result = gpd.read_parquet(io.BytesIO(parquet_data))
    print(gdf_result.head())
    assert len(gdf_result) == 1
    assert 'geometry' in gdf_result.columns


def test_convert_gpkg_to_parquet(sample_geodf, mock_boto3_client):
    with tempfile.TemporaryDirectory() as tmpdir:
        gpkg_path = os.path.join(tmpdir, "data.gpkg")
        sample_geodf.to_file(gpkg_path, driver="GPKG")
        
        with open(gpkg_path, 'rb') as f:
            gpkg_bytes = f.read()
    
    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(gpkg_bytes)
    }
    
    convert_gpkg_to_parquet("test_files/data.gpkg")
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    parquet_data = put_call[1]['Body']
    gdf_result = gpd.read_parquet(io.BytesIO(parquet_data))
    print(gdf_result.head())
    assert len(gdf_result) == 3
    assert 'geometry' in gdf_result.columns


def test_convert_dwg_to_parquet(mock_boto3_client):
    dwg_content = b'AC1032'  
    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(dwg_content)
    }
    
    with pytest.raises((OSError, NotImplementedError)):
        convert_dwg_to_parquet("test_files/drawing.dwg")
    assert mock_boto3_client.get_object.called


def test_convert_dxf_to_parquet(mock_boto3_client):
    doc = ezdxf.new('R2010')
    msp = doc.modelspace()
    msp.add_line((0, 0), (10, 0))
    msp.add_circle((5, 5), radius=2)
    
    dxf_string_buffer = io.StringIO()
    doc.write(dxf_string_buffer)
    dxf_bytes = dxf_string_buffer.getvalue().encode('utf-8')
    
    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(dxf_bytes)
    }
    
    convert_dxf_to_parquet("test_files/drawing.dxf")
    
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    parquet_data = put_call[1]['Body']
    df_result = pd.read_parquet(io.BytesIO(parquet_data))
    print(df_result.head())
    assert len(df_result) == 2


def test_convert_dxf_to_parquet_with_text(mock_boto3_client):
    doc = ezdxf.new('R2010')
    msp = doc.modelspace()
    msp.add_text("Hello", dxfattribs={'height': 0.5})
    msp.add_line((0, 0), (5, 5))
    
    dxf_string_buffer = io.StringIO()
    doc.write(dxf_string_buffer)
    dxf_bytes = dxf_string_buffer.getvalue().encode('utf-8')
    
    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(dxf_bytes)
    }
    
    convert_dxf_to_parquet("test_files/text.dxf")
    
    assert mock_boto3_client.put_object.called