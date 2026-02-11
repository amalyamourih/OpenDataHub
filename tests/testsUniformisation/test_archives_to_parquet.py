"""
Tests de conversion des formats d'archives vers Parquet
Formats: ZIP, TAR, TGZ, GZ, XZ, BZ2, 7Z, RAR
"""

import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

import pytest
import pandas as pd
import io
import os
import tempfile
import zipfile
import tarfile
import gzip
import lzma
import bz2


from transformation.transformat_files_to_parquet.convert_to_parquet.archive.zip import convert_zip_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.archive.tar import convert_tar_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.archive.tgz import convert_tgz_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.archive.gz import convert_gz_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.archive.xz import convert_xz_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.archive.bz2 import convert_bz2_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.archive._7z import convert_7z_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.archive.rar import convert_rar_to_parquet



def create_test_csv_content():
    """Crée un contenu CSV de test"""
    df = pd.DataFrame({'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie']})
    buffer = io.BytesIO()
    df.to_csv(buffer, index=False)
    return buffer.getvalue()


def test_convert_zip_to_parquet(mock_boto3_client):
    csv_content = create_test_csv_content()
    
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('data.csv', csv_content)
    zip_buffer.seek(0)
    zip_bytes = zip_buffer.getvalue()
    
    # Mock qui retourne différents contenus selon la clé
    def mock_get_object(**kwargs):
        key = kwargs.get('Key', '')
        if key.endswith('.zip'):
            return {'Body': io.BytesIO(zip_bytes)}
        elif key.endswith('.csv'):
            return {'Body': io.BytesIO(csv_content)}
        return {'Body': io.BytesIO(b'')}
    
    mock_boto3_client.get_object.side_effect = mock_get_object
    
    convert_zip_to_parquet("test_files/archive.zip")
    
    assert mock_boto3_client.put_object.called
    put_calls = [call for call in mock_boto3_client.put_object.call_args_list 
                 if 'parquets_files' in call[1].get('Key', '')]
    assert len(put_calls) >= 1, "Aucun fichier Parquet généré"
    parquet_data = put_calls[-1][1]['Body']
    df_result = pd.read_parquet(io.BytesIO(parquet_data))
    print(df_result.head())
    assert len(df_result) == 3


def test_convert_zip_to_parquet_multiple_files(mock_boto3_client):
    csv_content = create_test_csv_content()
    
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('file1.csv', csv_content)
        zf.writestr('file2.csv', csv_content)
    zip_buffer.seek(0)
    zip_bytes = zip_buffer.getvalue()
    
    def mock_get_object(**kwargs):
        key = kwargs.get('Key', '')
        if key.endswith('.zip'):
            return {'Body': io.BytesIO(zip_bytes)}
        elif key.endswith('.csv'):
            return {'Body': io.BytesIO(csv_content)}
        return {'Body': io.BytesIO(b'')}
    
    mock_boto3_client.get_object.side_effect = mock_get_object
    
    convert_zip_to_parquet("test_files/multi.zip")
    
    assert mock_boto3_client.put_object.called
    put_calls = [call for call in mock_boto3_client.put_object.call_args_list 
                 if 'parquets_files' in call[1].get('Key', '')]
    assert len(put_calls) >= 2, "Devrait avoir 2 fichiers Parquet"
    
    for i, call in enumerate(put_calls):
        parquet_data = call[1]['Body']
        df_result = pd.read_parquet(io.BytesIO(parquet_data))
        print(f"Fichier {i+1}: {call[1]['Key']}")
        print(df_result.head())



# TODO : checker les warnings
def test_convert_tar_to_parquet(mock_boto3_client):
    csv_content = create_test_csv_content()
    
    tar_buffer = io.BytesIO()
    with tarfile.open(fileobj=tar_buffer, mode='w') as tf:
        csv_info = tarfile.TarInfo(name='data.csv')
        csv_info.size = len(csv_content)
        tf.addfile(csv_info, io.BytesIO(csv_content))
    tar_buffer.seek(0)
    tar_bytes = tar_buffer.getvalue()
    
    def mock_get_object(**kwargs):
        key = kwargs.get('Key', '')
        if key.endswith('.tar'):
            return {'Body': io.BytesIO(tar_bytes)}
        elif key.endswith('.csv'):
            return {'Body': io.BytesIO(csv_content)}
        return {'Body': io.BytesIO(b'')}
    
    mock_boto3_client.get_object.side_effect = mock_get_object
    
    convert_tar_to_parquet("test_files/archive.tar")
    
    assert mock_boto3_client.put_object.called
    put_calls = [call for call in mock_boto3_client.put_object.call_args_list 
                 if 'parquets_files' in call[1].get('Key', '')]
    assert len(put_calls) >= 1, "Aucun fichier Parquet généré"
    parquet_data = put_calls[-1][1]['Body']
    df_result = pd.read_parquet(io.BytesIO(parquet_data))
    print(df_result.head())
    assert len(df_result) == 3


# TODO : checker les warnings
def test_convert_tgz_to_parquet(mock_boto3_client):
    csv_content = create_test_csv_content()
    
    tgz_buffer = io.BytesIO()
    with tarfile.open(fileobj=tgz_buffer, mode='w:gz') as tf:
        csv_info = tarfile.TarInfo(name='data.csv')
        csv_info.size = len(csv_content)
        tf.addfile(csv_info, io.BytesIO(csv_content))
    tgz_buffer.seek(0)
    tgz_bytes = tgz_buffer.getvalue()
    
    def mock_get_object(**kwargs):
        key = kwargs.get('Key', '')
        if key.endswith('.tgz'):
            return {'Body': io.BytesIO(tgz_bytes)}
        elif key.endswith('.csv'):
            return {'Body': io.BytesIO(csv_content)}
        return {'Body': io.BytesIO(b'')}
    
    mock_boto3_client.get_object.side_effect = mock_get_object
    
    convert_tgz_to_parquet("test_files/archive.tgz")
    
    assert mock_boto3_client.put_object.called
    put_calls = [call for call in mock_boto3_client.put_object.call_args_list 
                 if 'parquets_files' in call[1].get('Key', '')]
    assert len(put_calls) >= 1, "Aucun fichier Parquet généré"
    parquet_data = put_calls[-1][1]['Body']
    df_result = pd.read_parquet(io.BytesIO(parquet_data))
    print(df_result.head())
    assert len(df_result) == 3



def test_convert_gz_to_parquet(mock_boto3_client):
    csv_content = create_test_csv_content()
    gz_content = gzip.compress(csv_content)
    
    def mock_get_object(**kwargs):
        key = kwargs.get('Key', '')
        if key.endswith('.gz'):
            return {'Body': io.BytesIO(gz_content)}
        elif key.endswith('.csv'):
            return {'Body': io.BytesIO(csv_content)}
        return {'Body': io.BytesIO(b'')}
    
    mock_boto3_client.get_object.side_effect = mock_get_object
    
    convert_gz_to_parquet("test_files/data.csv.gz")
    
    assert mock_boto3_client.put_object.called
    put_calls = [call for call in mock_boto3_client.put_object.call_args_list 
                 if 'parquets_files' in call[1].get('Key', '')]
    assert len(put_calls) >= 1, "Aucun fichier Parquet généré"
    parquet_data = put_calls[-1][1]['Body']
    df_result = pd.read_parquet(io.BytesIO(parquet_data))
    print(df_result.head())
    assert len(df_result) == 3


def test_convert_xz_to_parquet(mock_boto3_client):
    csv_content = create_test_csv_content()
    xz_content = lzma.compress(csv_content)
    
    def mock_get_object(**kwargs):
        key = kwargs.get('Key', '')
        if key.endswith('.xz'):
            return {'Body': io.BytesIO(xz_content)}
        elif key.endswith('.csv'):
            return {'Body': io.BytesIO(csv_content)}
        return {'Body': io.BytesIO(b'')}
    
    mock_boto3_client.get_object.side_effect = mock_get_object
    
    convert_xz_to_parquet("test_files/data.csv.xz")
    
    assert mock_boto3_client.put_object.called
    put_calls = [call for call in mock_boto3_client.put_object.call_args_list 
                 if 'parquets_files' in call[1].get('Key', '')]
    assert len(put_calls) >= 1, "Aucun fichier Parquet généré"
    parquet_data = put_calls[-1][1]['Body']
    df_result = pd.read_parquet(io.BytesIO(parquet_data))
    print(df_result.head())
    assert len(df_result) == 3


def test_convert_bz2_to_parquet(mock_boto3_client):
    csv_content = create_test_csv_content()
    bz2_content = bz2.compress(csv_content)
    
    def mock_get_object(**kwargs):
        key = kwargs.get('Key', '')
        if key.endswith('.bz2'):
            return {'Body': io.BytesIO(bz2_content)}
        elif key.endswith('.csv'):
            return {'Body': io.BytesIO(csv_content)}
        return {'Body': io.BytesIO(b'')}
    
    mock_boto3_client.get_object.side_effect = mock_get_object
    
    convert_bz2_to_parquet("test_files/data.csv.bz2")
    
    assert mock_boto3_client.put_object.called
    put_calls = [call for call in mock_boto3_client.put_object.call_args_list 
                 if 'parquets_files' in call[1].get('Key', '')]
    assert len(put_calls) >= 1, "Aucun fichier Parquet généré"
    parquet_data = put_calls[-1][1]['Body']
    df_result = pd.read_parquet(io.BytesIO(parquet_data))
    print(df_result.head())
    assert len(df_result) == 3


def test_convert_7z_to_parquet(mock_boto3_client):
    py7zr = pytest.importorskip("py7zr")
    csv_content = create_test_csv_content()
    
    with tempfile.TemporaryDirectory() as tmpdir:
        archive_path = os.path.join(tmpdir, "archive.7z")
        with py7zr.SevenZipFile(archive_path, 'w') as archive:
            archive.writestr(csv_content, 'data.csv')
        
        with open(archive_path, 'rb') as f:
            sz_bytes = f.read()
    
    def mock_get_object(**kwargs):
        key = kwargs.get('Key', '')
        if key.endswith('.7z'):
            return {'Body': io.BytesIO(sz_bytes)}
        elif key.endswith('.csv'):
            return {'Body': io.BytesIO(csv_content)}
        return {'Body': io.BytesIO(b'')}
    
    mock_boto3_client.get_object.side_effect = mock_get_object
    
    convert_7z_to_parquet("test_files/archive.7z")
    
    assert mock_boto3_client.put_object.called
    put_calls = [call for call in mock_boto3_client.put_object.call_args_list 
                 if 'parquets_files' in call[1].get('Key', '')]
    assert len(put_calls) >= 1, "Aucun fichier Parquet généré"
    parquet_data = put_calls[-1][1]['Body']
    df_result = pd.read_parquet(io.BytesIO(parquet_data))
    print(df_result.head())
    assert len(df_result) == 3


def test_convert_rar_to_parquet(mock_boto3_client):
    pytest.skip("RAR nécessite unrar installé sur le système")