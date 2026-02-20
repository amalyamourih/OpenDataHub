import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

import pytest
import pandas as pd
import io
import tempfile
import os
from unittest.mock import patch, MagicMock
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.csv import convert_csv_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.tsv import convert_tsv_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.xls import convert_xls_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.xlsx import convert_xlsx_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.ods import convert_ods_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.txt import convert_txt_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.dbf import convert_dbf_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.parquet import convert_parquet_to_parquet
from transformation.transformat_files_to_parquet.convert_to_parquet.tabular.pq import convert_pq_to_parquet



pytest.importorskip("dbfread")
import xlwt


def test_convert_csv_to_parquet(sample_df, mock_boto3_client):
    csv_buffer = io.BytesIO()
    sample_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(csv_buffer.getvalue())
    }
    
    convert_csv_to_parquet("test_files/data.csv", S3_BUCKET="test-bucket")
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    parquet_data = put_call[1]['Body']
    df_result = pd.read_parquet(io.BytesIO(parquet_data))
    print(df_result.head())
    pd.testing.assert_frame_equal(df_result, sample_df)
    assert put_call[1]['Key'] == "parquets_files/data.parquet"

def test_convert_csv_to_parquet_with_special_chars(mock_boto3_client):
    df = pd.DataFrame({
        'nom': ['Éloïse', 'François', 'Müller'],
        'ville': ['Paris', 'Zürich', 'Montréal']
    })
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8')
    csv_buffer.seek(0)
    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(csv_buffer.getvalue())
    }
    convert_csv_to_parquet("test_files/special.csv", S3_BUCKET="test-bucket")
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    parquet_data = put_call[1]['Body']
    df_result = pd.read_parquet(io.BytesIO(parquet_data))
    print(df_result.head())
    pd.testing.assert_frame_equal(df_result, df)
    assert put_call[1]['Key'] == "parquets_files/special.parquet"

def test_convert_csv_to_parquet_empty_file(mock_boto3_client):
    df = pd.DataFrame(columns=['id', 'nom', 'age'])
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(csv_buffer.getvalue())
    }
    convert_csv_to_parquet("test_files/empty.csv", S3_BUCKET="test-bucket")
    assert mock_boto3_client.put_object.called



def test_convert_tsv_to_parquet(sample_df, mock_s3_client):
    tsv_buffer = io.BytesIO()
    sample_df.to_csv(tsv_buffer, index=False, sep='\t')
    tsv_buffer.seek(0)
    
    mock_s3_client.get_object.return_value = {
        'Body': io.BytesIO(tsv_buffer.getvalue())
    }
    
    with patch('ingestion.s3.io.get_s3_client') as mock_boto_client:
        mock_boto_client.return_value = mock_s3_client
        convert_tsv_to_parquet("test_files/data.tsv")
        assert mock_s3_client.put_object.called
        put_call = mock_s3_client.put_object.call_args
        parquet_data = put_call[1]['Body']
        df_result = pd.read_parquet(io.BytesIO(parquet_data))
        print(df_result.head())
        pd.testing.assert_frame_equal(df_result, sample_df)
        assert put_call[1]['Key'] == "parquets_files/data.parquet"


def test_convert_xls_to_parquet(sample_df, temp_s3_bucket_mock):
    try:
        xls_buffer = io.BytesIO()
        workbook = xlwt.Workbook()
        worksheet = workbook.add_sheet('data')
        
        for col_idx, col_name in enumerate(sample_df.columns):
            worksheet.write(0, col_idx, col_name)
        
        for row_idx, (_, row) in enumerate(sample_df.iterrows(), 1):
            for col_idx, value in enumerate(row):
                print(f"Writing value: {value} at row {row_idx}, column {col_idx}")
                worksheet.write(row_idx, col_idx, value)
        
        workbook.save(xls_buffer)
        xls_buffer.seek(0)
        xls_data = xls_buffer.getvalue()
    except ImportError:
        pytest.skip("xlwt non installé")
    
    with patch('ingestion.s3.io.get_s3_client') as mock_boto_client:
        def custom_get_object(**kwargs):
            key = kwargs.get('Key', '')
            if 'data.xls' in key:
                return {'Body': io.BytesIO(xls_data)}
            return temp_s3_bucket_mock.get_object(**kwargs)
        
        mock_boto_client.return_value = temp_s3_bucket_mock
        temp_s3_bucket_mock.get_object = custom_get_object
        convert_xls_to_parquet("test_files/data.xls")
        assert temp_s3_bucket_mock.put_object.called
        put_call = temp_s3_bucket_mock.put_object.call_args
        assert put_call[1]['Key'] == "parquets_files/data.parquet"

def test_convert_xlsx_to_parquet(sample_df, mock_boto3_client):
    xlsx_buffer = io.BytesIO()
    with pd.ExcelWriter(xlsx_buffer, engine='xlsxwriter') as writer:
        sample_df.to_excel(writer, sheet_name='data', index=False)
    xlsx_buffer.seek(0)

    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(xlsx_buffer.getvalue())
    }
    
    convert_xlsx_to_parquet("test_files/data.xlsx")
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    parquet_data = put_call[1]['Body']
    df_result = pd.read_parquet(io.BytesIO(parquet_data))
    print(df_result.head())
    pd.testing.assert_frame_equal(df_result, sample_df)

def test_convert_xlsx_multiple_sheets(mock_boto3_client):
    df1 = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    df2 = pd.DataFrame({'col3': [3, 4], 'col4': ['c', 'd']})
    
    xlsx_buffer = io.BytesIO()
    with pd.ExcelWriter(xlsx_buffer, engine='xlsxwriter') as writer:
        df1.to_excel(writer, sheet_name='Sheet1', index=False)
        df2.to_excel(writer, sheet_name='Sheet2', index=False)
    xlsx_buffer.seek(0)
    
    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(xlsx_buffer.getvalue())
    }
    
    convert_xlsx_to_parquet("test_files/multi_sheet.xlsx")
    assert mock_boto3_client.put_object.call_count == 2
    put_calls = mock_boto3_client.put_object.call_args_list
    df_result1 = pd.read_parquet(io.BytesIO(put_calls[0][1]['Body']))
    df_result2 = pd.read_parquet(io.BytesIO(put_calls[1][1]['Body']))
    print(df_result1.head())
    print(df_result2.head())
    pd.testing.assert_frame_equal(df_result1, df1)
    pd.testing.assert_frame_equal(df_result2, df2)




def test_convert_txt_to_parquet_space_separated(sample_df, mock_s3_client):
    txt_buffer = io.BytesIO()
    sample_df.to_csv(txt_buffer, index=False, sep=' ')
    txt_buffer.seek(0)

    mock_s3_client.get_object.return_value = {
        'Body': io.BytesIO(txt_buffer.getvalue())
    }
    
    with patch('ingestion.s3.io.get_s3_client') as mock_boto_client:
        mock_boto_client.return_value = mock_s3_client
        convert_txt_to_parquet("test_files/data.txt")
        
        assert mock_s3_client.put_object.called
        put_call = mock_s3_client.put_object.call_args
        parquet_data = put_call[1]['Body']
        df_result = pd.read_parquet(io.BytesIO(parquet_data))
        print(df_result.head())
        pd.testing.assert_frame_equal(df_result, sample_df)
        assert put_call[1]['Key'] == "parquets_files/data.parquet"


def test_convert_txt_to_parquet_comma_separated(sample_df, mock_s3_client):
    txt_buffer = io.BytesIO()
    sample_df.to_csv(txt_buffer, index=False, sep=',')
    txt_buffer.seek(0)
    
    mock_s3_client.get_object.return_value = {
        'Body': io.BytesIO(txt_buffer.getvalue())
    }
    
    with patch('ingestion.s3.io.get_s3_client') as mock_boto_client:
        mock_boto_client.return_value = mock_s3_client
        convert_txt_to_parquet("test_files/data_comma.txt")
        assert mock_s3_client.put_object.called
        put_call = mock_s3_client.put_object.call_args
        parquet_data = put_call[1]['Body']
        df_result = pd.read_parquet(io.BytesIO(parquet_data))
        print(df_result.head())
        pd.testing.assert_frame_equal(df_result, sample_df)
        assert put_call[1]['Key'] == "parquets_files/data_comma.parquet"



def test_convert_ods_to_parquet(sample_df, mock_boto3_client):
    ods_buffer = io.BytesIO()
    with pd.ExcelWriter(ods_buffer, engine='odf') as writer:
        sample_df.to_excel(writer, index=False)
    ods_buffer.seek(0)
    ods_data = ods_buffer.getvalue()
    
    def mock_get_object(**kwargs):
        key = kwargs.get('Key', '')
        if 'data.ods' in key:
            return {'Body': io.BytesIO(ods_data)}
        return {'Body': io.BytesIO(b'')}
    
    mock_boto3_client.get_object.side_effect = mock_get_object
    
    convert_ods_to_parquet("test_files/data.ods")
    
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    parquet_data = put_call[1]['Body']
    df_result = pd.read_parquet(io.BytesIO(parquet_data))
    print(df_result.head())
    assert df_result.shape == sample_df.shape


def test_convert_dbf_to_parquet(sample_df, mock_boto3_client):
    with tempfile.TemporaryDirectory() as tmpdir:
        dbf_path = os.path.join(tmpdir, "data.dbf")
        try:
            import geopandas as gpd
            from shapely.geometry import Point
            gdf = gpd.GeoDataFrame(sample_df, geometry=[Point(0, 0)] * len(sample_df), crs='EPSG:4326')
            gdf.to_file(dbf_path.replace('.dbf', '.shp'))
            
            with open(dbf_path, 'rb') as f:
                dbf_bytes = f.read()
        except Exception:
            pytest.skip("Impossible de créer un fichier DBF de test")
    
    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(dbf_bytes)
    }
    
    convert_dbf_to_parquet("test_files/data.dbf")
    assert mock_boto3_client.put_object.called
    put_call = mock_boto3_client.put_object.call_args
    parquet_data = put_call[1]['Body']
    df_result = pd.read_parquet(io.BytesIO(parquet_data))
    print(df_result.head())
    assert put_call[1]['Key'] == "parquets_files/data.parquet"



def test_convert_parquet_to_parquet(sample_df, mock_s3_client):
    parquet_buffer = io.BytesIO()
    sample_df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    mock_s3_client.get_object.return_value = {
        'Body': io.BytesIO(parquet_buffer.getvalue())
    }
    with patch('ingestion.s3.io.get_s3_client') as mock_boto_client:
        mock_boto_client.return_value = mock_s3_client
        convert_parquet_to_parquet("test_files/data.parquet")
        assert mock_s3_client.put_object.called
        put_call = mock_s3_client.put_object.call_args
        assert put_call[1]['Key'] == "parquets_files/data.parquet"


def test_convert_pq_to_parquet(sample_df, mock_s3_client):
    parquet_buffer = io.BytesIO()
    sample_df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    
    mock_s3_client.get_object.return_value = {
        'Body': io.BytesIO(parquet_buffer.getvalue())
    }
    with patch('ingestion.s3.io.get_s3_client') as mock_boto_client:
        mock_boto_client.return_value = mock_s3_client
        convert_pq_to_parquet("test_files/data.pq")
        assert mock_s3_client.put_object.called
        put_call = mock_s3_client.put_object.call_args
        assert put_call[1]['Key'] == "parquets_files/data.parquet"