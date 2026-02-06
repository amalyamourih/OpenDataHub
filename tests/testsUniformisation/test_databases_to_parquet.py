import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

import pytest
import pandas as pd
import sqlite3
import io
import os
import tempfile

from transformation.transforme_with_duckdb.transforme_to_parquet import (
    convert_sql_to_parquet,
    convert_db_to_parquet_all_tables
)


def create_test_sqlite_db(tables_data):
    # create a temporary file path, close the NamedTemporaryFile before using it on Windows
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_path = tmp.name

    conn = sqlite3.connect(tmp_path)
    for table_name, df in tables_data.items():
        df.to_sql(table_name, conn, index=False, if_exists='replace')
    conn.close()

    with open(tmp_path, 'rb') as f:
        db_bytes = f.read()

    os.unlink(tmp_path)
    return db_bytes


def test_convert_sql_to_parquet(mock_boto3_client):
    sql_content = """
    CREATE TABLE users (id INTEGER, name TEXT, age INTEGER);
    INSERT INTO users VALUES (1, 'Alice', 25);
    INSERT INTO users VALUES (2, 'Bob', 30);
    
    CREATE TABLE products (id INTEGER, name TEXT, price REAL);
    INSERT INTO products VALUES (1, 'Laptop', 999.99);
    """
    
    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(sql_content.encode('utf-8'))
    }
    
    convert_sql_to_parquet("test_files/data.sql")
    assert mock_boto3_client.put_object.call_count == 2
    put_obj = mock_boto3_client.put_object.call_args
    put_calls = mock_boto3_client.put_object.call_args_list
    parquet_data_1 = put_calls[0][1]['Body']
    df_result_1 = pd.read_parquet(io.BytesIO(parquet_data_1))
    print(df_result_1.head())

    parquet_data_2 = put_calls[1][1]['Body']
    df_result_2 = pd.read_parquet(io.BytesIO(parquet_data_2))
    print(df_result_2.head())


def test_convert_sql_to_parquet_single_table(mock_boto3_client):
    sql_content = """
    CREATE TABLE employees (id INTEGER, name TEXT, department TEXT);
    INSERT INTO employees VALUES (1, 'Alice', 'Engineering');
    INSERT INTO employees VALUES (2, 'Bob', 'Marketing');
    """
    
    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(sql_content.encode('utf-8'))
    }
    
    convert_sql_to_parquet("test_files/employees.sql")
    
    assert mock_boto3_client.put_object.called
    put_obj = mock_boto3_client.put_object.call_args
    parquet_data = put_obj[1]['Body']
    df_result = pd.read_parquet(io.BytesIO(parquet_data))
    print(df_result.head())


def test_convert_sql_to_parquet_empty_table(mock_boto3_client):
    sql_content = "CREATE TABLE empty_table (id INTEGER, data TEXT);"
    
    mock_boto3_client.get_object.return_value = {
        'Body': io.BytesIO(sql_content.encode('utf-8'))
    }
    
    convert_sql_to_parquet("test_files/empty.sql")
    
    assert mock_boto3_client.put_object.called
    put_obj = mock_boto3_client.put_object.call_args
    parquet_data = put_obj[1]['Body']
    df_result = pd.read_parquet(io.BytesIO(parquet_data))
    print(df_result.head())


def test_convert_db_to_parquet_all_tables(sample_df, mock_boto3_client):
    tables_data = {
        'users': sample_df,
        'orders': pd.DataFrame({
            'order_id': [1, 2, 3],
            'user_id': [1, 2, 1],
            'amount': [100.0, 200.0, 150.0]
        })
    }
    
    db_bytes = create_test_sqlite_db(tables_data)
    
    mock_boto3_client.get_object.return_value = {'Body': io.BytesIO(db_bytes)}
    
    convert_db_to_parquet_all_tables("test_files/data.db")
    
    assert mock_boto3_client.put_object.call_count == 2
    put_obj = mock_boto3_client.put_object.call_args
    parquet_data = put_obj[1]['Body']
    df_result = pd.read_parquet(io.BytesIO(parquet_data))
    print(df_result.head())


def test_convert_db_to_parquet_single_table(mock_boto3_client):
    df = pd.DataFrame({'id': [1, 2, 3], 'value': ['a', 'b', 'c']})
    db_bytes = create_test_sqlite_db({'single_table': df})
    
    mock_boto3_client.get_object.return_value = {'Body': io.BytesIO(db_bytes)}
    
    convert_db_to_parquet_all_tables("test_files/single.db")
    
    assert mock_boto3_client.put_object.called
    put_obj = mock_boto3_client.put_object.call_args
    parquet_data = put_obj[1]['Body']
    df_result = pd.read_parquet(io.BytesIO(parquet_data))
    print(df_result.head())


def test_convert_db_to_parquet_large_table(mock_boto3_client):
    df = pd.DataFrame({
        'id': range(1000),
        'value': [f'value_{i}' for i in range(1000)]
    })
    db_bytes = create_test_sqlite_db({'large_table': df})
    
    mock_boto3_client.get_object.return_value = {'Body': io.BytesIO(db_bytes)}
    
    convert_db_to_parquet_all_tables("test_files/large.db")
    assert mock_boto3_client.put_object.called
    put_obj = mock_boto3_client.put_object.call_args
    parquet_data = put_obj[1]['Body']
    df_result = pd.read_parquet(io.BytesIO(parquet_data))
    print(df_result.head())